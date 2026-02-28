use crate::b64::encoder::utilities::container_builder::{
    BLOCK_DIRECTORY_ENTRY_SIZE, BlockDirEntry, FilterType, Stride,
};
use crate::b64::utilities::common::{decompress_zstd, read_u32_le_at, read_u64_le_at, take};
use crate::mzml::structs::NumericType;
use std::ops::Deref;

pub(crate) trait BlockProcessor {
    fn decompress(&self, source: &[u8], target_len: usize) -> Result<Vec<u8>, String>;
    fn unshuffle(&self, source: &[u8], target: &mut [u8], stride: usize);
    fn requires_unshuffle(&self, filter: FilterType) -> bool;
}

#[derive(Debug)]
pub(crate) struct DefaultProcessor;

impl BlockProcessor for DefaultProcessor {
    #[inline]
    fn decompress(&self, source: &[u8], target_len: usize) -> Result<Vec<u8>, String> {
        decompress_zstd(source, target_len)
    }

    #[inline]
    fn unshuffle(&self, source: &[u8], target: &mut [u8], stride: usize) {
        unshuffle_bytes(source, target, stride);
    }

    #[inline]
    fn requires_unshuffle(&self, filter: FilterType) -> bool {
        filter == FilterType::Shuffle
    }
}

#[derive(Debug)]
pub(crate) enum BlockData<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> Deref for BlockData<'a> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(data) => data,
            Self::Owned(data) => data.as_slice(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ContainerView<'a, P: BlockProcessor> {
    raw_data: &'a [u8],
    entries: Vec<BlockDirEntry>,
    cache: Vec<Option<BlockData<'a>>>,
    scratch_buffer: Vec<u8>,
    stride_history: Vec<Option<Stride>>,
    compression_level: u8,
    filter: FilterType,
    processor: P,
}

impl<'a, P: BlockProcessor> ContainerView<'a, P> {
    pub(crate) fn new(
        raw_data: &'a [u8],
        block_count: u32,
        compression_level: u8,
        filter: FilterType,
        ctx: &'static str,
        processor: P,
    ) -> Result<Self, String> {
        let block_count = block_count as usize;
        let directory_byte_size = block_count * BLOCK_DIRECTORY_ENTRY_SIZE;

        if raw_data.len() < directory_byte_size {
            return Err(format!(
                "{ctx}: container too small to hold block directory"
            ));
        }

        let directory_start_offset = raw_data.len() - directory_byte_size;
        let directory_bytes = &raw_data[directory_start_offset..];
        let mut read_position = 0;
        let mut entries = Vec::with_capacity(block_count);

        for _ in 0..block_count {
            let payload_offset = read_u64_le_at(directory_bytes, &mut read_position, ctx)?;
            let payload_size = read_u64_le_at(directory_bytes, &mut read_position, ctx)?;
            let uncompressed_len_bytes = read_u64_le_at(directory_bytes, &mut read_position, ctx)?;
            let _reserved_padding = take(directory_bytes, &mut read_position, 8, ctx)?;
            entries.push(BlockDirEntry {
                payload_offset,
                payload_size,
                uncompressed_len_bytes,
            });
        }

        let mut cache = Vec::with_capacity(block_count);
        cache.resize_with(block_count, || None);

        Ok(Self {
            raw_data,
            entries,
            cache,
            scratch_buffer: Vec::new(),
            stride_history: vec![None; block_count],
            compression_level,
            filter,
            processor,
        })
    }

    #[inline]
    pub(crate) fn get_item_from_block(
        &mut self,
        block_id: u32,
        element_offset: u64,
        element_count: u64,
        element_stride: usize,
        ctx: &'static str,
    ) -> Result<&[u8], String> {
        self.ensure_block_loaded(block_id, element_stride, ctx)?;

        let block = self.cache[block_id as usize].as_ref().unwrap();
        let start_byte = (element_offset as usize) * element_stride;
        let end_byte = start_byte + (element_count as usize) * element_stride;

        if end_byte > block.len() {
            return Err(format!(
                "{ctx}: item range [{start_byte}..{end_byte}] out of bounds for block {block_id} (len={})",
                block.len()
            ));
        }
        Ok(&block[start_byte..end_byte])
    }

    fn ensure_block_loaded(
        &mut self,
        block_id: u32,
        element_stride: usize,
        ctx: &'static str,
    ) -> Result<(), String> {
        let block_index = block_id as usize;
        if block_index >= self.cache.len() {
            return Err(format!(
                "{ctx}: block index {block_index} out of range (count={})",
                self.cache.len()
            ));
        }
        if self.cache[block_index].is_some() {
            return Ok(());
        }

        let stride = Stride::from_size(element_stride);
        self.record_stride_or_fail(block_index, stride, ctx)?;

        let entry = self.entries[block_index];
        let payload_start = entry.payload_offset as usize;
        let payload_end = payload_start
            .checked_add(entry.payload_size as usize)
            .ok_or_else(|| format!("{ctx}: block {block_index} payload size overflows"))?;

        if payload_end > self.raw_data.len() - (self.entries.len() * BLOCK_DIRECTORY_ENTRY_SIZE) {
            return Err(format!(
                "{ctx}: block {block_index} payload exceeds payload region bounds"
            ));
        }

        let decoded = self.run_decode_pipeline(
            &self.raw_data[payload_start..payload_end],
            entry.uncompressed_len_bytes as usize,
            stride,
        )?;
        self.cache[block_index] = Some(decoded);
        Ok(())
    }

    fn record_stride_or_fail(
        &mut self,
        block_index: usize,
        stride: Stride,
        ctx: &'static str,
    ) -> Result<(), String> {
        if !self.processor.requires_unshuffle(self.filter) || stride == Stride::OneByte {
            return Ok(());
        }
        match self.stride_history[block_index] {
            None => {
                self.stride_history[block_index] = Some(stride);
                Ok(())
            }
            Some(recorded) if recorded == stride => Ok(()),
            Some(recorded) => Err(format!(
                "{ctx}: stride mismatch for block {block_index} (expected {recorded:?}, got {stride:?})"
            )),
        }
    }

    fn run_decode_pipeline(
        &mut self,
        payload: &'a [u8],
        uncompressed_len: usize,
        stride: Stride,
    ) -> Result<BlockData<'a>, String> {
        let needs_unshuffle =
            self.processor.requires_unshuffle(self.filter) && stride != Stride::OneByte;

        if self.compression_level == 0 && !needs_unshuffle {
            if payload.len() != uncompressed_len {
                return Err(format!(
                    "uncompressed payload size mismatch: got {}, expected {uncompressed_len}",
                    payload.len()
                ));
            }
            return Ok(BlockData::Borrowed(payload));
        }

        let mut decompressed = if self.compression_level == 0 {
            payload.to_vec()
        } else {
            self.processor.decompress(payload, uncompressed_len)?
        };

        if needs_unshuffle {
            self.scratch_buffer.resize(uncompressed_len, 0);
            self.processor
                .unshuffle(&decompressed, &mut self.scratch_buffer, stride.as_usize());
            std::mem::swap(&mut decompressed, &mut self.scratch_buffer);
        }

        Ok(BlockData::Owned(decompressed))
    }
}

#[inline(always)]
fn unshuffle_bytes(source: &[u8], target: &mut [u8], stride: usize) {
    match stride {
        8 => unshuffle8(source, target),
        4 => unshuffle4(source, target),
        2 => unshuffle2(source, target),
        _ => unshuffle_any(source, target, stride),
    }
}

#[inline(always)]
fn unshuffle2(source: &[u8], target: &mut [u8]) {
    let half = source.len() / 2;
    let (first_half, second_half) = source.split_at(half);
    for i in 0..half {
        target[i * 2] = first_half[i];
        target[i * 2 + 1] = second_half[i];
    }
}

#[inline(always)]
fn unshuffle4(source: &[u8], target: &mut [u8]) {
    let quarter = source.len() / 4;
    let (g0, rest) = source.split_at(quarter);
    let (g1, rest) = rest.split_at(quarter);
    let (g2, g3) = rest.split_at(quarter);
    for i in 0..quarter {
        let o = i * 4;
        target[o] = g0[i];
        target[o + 1] = g1[i];
        target[o + 2] = g2[i];
        target[o + 3] = g3[i];
    }
}

#[inline(always)]
fn unshuffle8(source: &[u8], target: &mut [u8]) {
    let seg = source.len() / 8;
    let (g0, rest) = source.split_at(seg);
    let (g1, rest) = rest.split_at(seg);
    let (g2, rest) = rest.split_at(seg);
    let (g3, rest) = rest.split_at(seg);
    let (g4, rest) = rest.split_at(seg);
    let (g5, rest) = rest.split_at(seg);
    let (g6, g7) = rest.split_at(seg);
    for i in 0..seg {
        let o = i * 8;
        target[o] = g0[i];
        target[o + 1] = g1[i];
        target[o + 2] = g2[i];
        target[o + 3] = g3[i];
        target[o + 4] = g4[i];
        target[o + 5] = g5[i];
        target[o + 6] = g6[i];
        target[o + 7] = g7[i];
    }
}

#[inline(always)]
fn unshuffle_any(source: &[u8], target: &mut [u8], stride: usize) {
    let element_count = source.len() / stride;
    for byte_position in 0..stride {
        let source_base = byte_position * element_count;
        for element_index in 0..element_count {
            target[byte_position + element_index * stride] = source[source_base + element_index];
        }
    }
}

pub(crate) struct BinaryStoreConfig {
    pub(crate) block_count: u32,
    pub(crate) item_count: u32,
    pub(crate) compression_level: u8,
    pub(crate) filter: FilterType,
    pub(crate) context_label: &'static str,
}

pub(crate) struct BinaryStore {
    slots: Vec<Option<Vec<(u32, ArrayData)>>>,
}

struct ArrayRef {
    array_type_accession: u32,
    dtype: u8,
    block_id: u32,
    element_offset: u64,
    element_count: u64,
}

struct ItemIndexEntry {
    arrayref_start: u64,
    arrayref_count: u64,
}

pub(crate) const ARRAYREF_ENTRY_BYTE_SIZE: u64 = 32;

#[derive(Clone, Debug)]
pub(crate) enum ArrayData {
    F64(Vec<f64>),
    F32(Vec<f32>),
    F16(Vec<u16>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

impl BinaryStore {
    pub(crate) fn build(
        container_bytes: &[u8],
        arrayref_bytes: &[u8],
        item_index_bytes: &[u8],
        config: BinaryStoreConfig,
    ) -> Result<Self, String> {
        let mut view = ContainerView::new(
            container_bytes,
            config.block_count,
            config.compression_level,
            config.filter,
            config.context_label,
            DefaultProcessor,
        )?;

        let array_refs = Self::parse_arrayrefs(arrayref_bytes)?;
        let item_index = Self::parse_item_index(item_index_bytes, config.item_count)?;

        let slots = item_index
            .iter()
            .map(|entry| {
                Some(Self::extract_arrays_for_entry(
                    &mut view,
                    &array_refs,
                    entry,
                ))
            })
            .collect();

        Ok(Self { slots })
    }

    #[inline]
    pub(crate) fn take(&mut self, slot_index: usize) -> Option<Vec<(u32, ArrayData)>> {
        self.slots.get_mut(slot_index)?.take()
    }

    fn parse_item_index(raw: &[u8], item_count: u32) -> Result<Vec<ItemIndexEntry>, String> {
        let mut read_pos = 0;
        let mut entries = Vec::with_capacity(item_count as usize);
        for _ in 0..item_count {
            let arrayref_start = read_u64_le_at(raw, &mut read_pos, "arrayref_start")?;
            let arrayref_count = read_u64_le_at(raw, &mut read_pos, "arrayref_count")?;
            entries.push(ItemIndexEntry {
                arrayref_start,
                arrayref_count,
            });
        }
        Ok(entries)
    }

    fn parse_arrayrefs(raw: &[u8]) -> Result<Vec<ArrayRef>, String> {
        let entry_count = (raw.len() as u64 / ARRAYREF_ENTRY_BYTE_SIZE) as usize;
        let mut read_pos = 0;
        let mut refs = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            let element_offset = read_u64_le_at(raw, &mut read_pos, "element_offset")?;
            let element_count = read_u64_le_at(raw, &mut read_pos, "element_count")?;
            let block_id = read_u32_le_at(raw, &mut read_pos, "block_id")?;
            let array_type_accession = read_u32_le_at(raw, &mut read_pos, "array_type")?;
            let dtype = take(raw, &mut read_pos, 1, "dtype")?[0];
            let _padding = take(raw, &mut read_pos, 7, "padding")?;
            refs.push(ArrayRef {
                array_type_accession,
                dtype,
                block_id,
                element_offset,
                element_count,
            });
        }
        Ok(refs)
    }

    fn extract_arrays_for_entry<P: BlockProcessor>(
        view: &mut ContainerView<'_, P>,
        array_refs: &[ArrayRef],
        entry: &ItemIndexEntry,
    ) -> Vec<(u32, ArrayData)> {
        let range_start = entry.arrayref_start as usize;
        let range_end = range_start + entry.arrayref_count as usize;
        if range_end > array_refs.len() {
            return Vec::new();
        }
        array_refs[range_start..range_end]
            .iter()
            .filter_map(|array_ref| {
                let (element_stride, numeric_type) =
                    Self::dtype_to_stride_and_type(array_ref.dtype).ok()?;
                let raw_bytes = view
                    .get_item_from_block(
                        array_ref.block_id,
                        array_ref.element_offset,
                        array_ref.element_count,
                        element_stride,
                        "",
                    )
                    .ok()?;
                Some((
                    array_ref.array_type_accession,
                    Self::bytes_to_typed_array(raw_bytes, numeric_type),
                ))
            })
            .collect()
    }

    #[inline]
    fn dtype_to_stride_and_type(dtype: u8) -> Result<(usize, NumericType), String> {
        match dtype {
            1 => Ok((8, NumericType::Float64)),
            2 => Ok((4, NumericType::Float32)),
            3 => Ok((2, NumericType::Float16)),
            4 => Ok((2, NumericType::Int16)),
            5 => Ok((4, NumericType::Int32)),
            6 => Ok((8, NumericType::Int64)),
            unknown => Err(format!("unrecognised dtype code {unknown}")),
        }
    }

    #[inline]
    fn bytes_to_typed_array(raw: &[u8], numeric_type: NumericType) -> ArrayData {
        match numeric_type {
            NumericType::Float64 => ArrayData::F64(byte_cast(raw)),
            NumericType::Float32 => ArrayData::F32(byte_cast(raw)),
            NumericType::Float16 => ArrayData::F16(byte_cast(raw)),
            NumericType::Int64 => ArrayData::I64(byte_cast(raw)),
            NumericType::Int32 => ArrayData::I32(byte_cast(raw)),
            NumericType::Int16 => ArrayData::I16(byte_cast(raw)),
        }
    }
}

#[inline]
fn byte_cast<T>(raw: &[u8]) -> Vec<T> {
    let element_count = raw.len() / std::mem::size_of::<T>();
    let mut out = Vec::with_capacity(element_count);
    unsafe {
        out.set_len(element_count);
        std::ptr::copy_nonoverlapping(raw.as_ptr(), out.as_mut_ptr() as *mut u8, raw.len());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_raw_directory_entry(
        payload_offset: u64,
        payload_size: u64,
        uncompressed: u64,
    ) -> Vec<u8> {
        let mut entry = Vec::with_capacity(BLOCK_DIRECTORY_ENTRY_SIZE);
        entry.extend_from_slice(&payload_offset.to_le_bytes());
        entry.extend_from_slice(&payload_size.to_le_bytes());
        entry.extend_from_slice(&uncompressed.to_le_bytes());
        entry.extend_from_slice(&[0u8; 8]);
        entry
    }

    #[test]
    fn container_view_rejects_data_smaller_than_directory() {
        let tiny = vec![0u8; 10];
        let result = ContainerView::new(&tiny, 1, 0, FilterType::None, "test", DefaultProcessor);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too small"));
    }

    #[test]
    fn container_view_accepts_empty_container_with_zero_blocks() {
        let empty = vec![];
        let result = ContainerView::new(&empty, 0, 0, FilterType::None, "test", DefaultProcessor);
        assert!(result.is_ok());
    }

    #[test]
    fn container_view_get_item_returns_correct_bytes_uncompressed() {
        let payload = vec![0u8, 1, 2, 3, 4, 5, 6, 7];
        let directory = make_raw_directory_entry(0, 8, 8);
        let mut raw = Vec::new();
        raw.extend_from_slice(&payload);
        raw.extend_from_slice(&directory);

        let mut view =
            ContainerView::new(&raw, 1, 0, FilterType::None, "test", DefaultProcessor).unwrap();
        let result = view.get_item_from_block(0, 1, 1, 4, "test").unwrap();
        assert_eq!(result, &[4u8, 5, 6, 7]);
    }

    #[test]
    fn container_view_get_item_returns_multiple_elements_uncompressed() {
        let payload = vec![0u8, 1, 2, 3, 4, 5, 6, 7];
        let directory = make_raw_directory_entry(0, 8, 8);
        let mut raw = Vec::new();
        raw.extend_from_slice(&payload);
        raw.extend_from_slice(&directory);

        let mut view =
            ContainerView::new(&raw, 1, 0, FilterType::None, "test", DefaultProcessor).unwrap();
        let result = view.get_item_from_block(0, 0, 2, 4, "test").unwrap();
        assert_eq!(result, &[0u8, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn container_view_rejects_out_of_bounds_element_range() {
        let payload = vec![0u8; 8];
        let directory = make_raw_directory_entry(0, 8, 8);
        let mut raw = Vec::new();
        raw.extend_from_slice(&payload);
        raw.extend_from_slice(&directory);

        let mut view =
            ContainerView::new(&raw, 1, 0, FilterType::None, "test", DefaultProcessor).unwrap();
        let result = view.get_item_from_block(0, 0, 3, 4, "test");
        assert!(result.is_err());
    }

    #[test]
    fn container_view_rejects_invalid_block_id() {
        let empty = vec![];
        let mut view =
            ContainerView::new(&empty, 0, 0, FilterType::None, "test", DefaultProcessor).unwrap();
        let result = view.get_item_from_block(99, 0, 1, 4, "test");
        assert!(result.is_err());
    }

    #[test]
    fn binary_store_take_returns_none_after_first_call() {
        let config = BinaryStoreConfig {
            block_count: 0,
            item_count: 0,
            compression_level: 0,
            filter: FilterType::None,
            context_label: "test",
        };
        let mut store = BinaryStore::build(&[], &[], &[], config).unwrap();
        assert!(store.take(0).is_none());
    }

    #[test]
    fn binary_store_build_with_empty_sections_succeeds() {
        let config = BinaryStoreConfig {
            block_count: 0,
            item_count: 0,
            compression_level: 0,
            filter: FilterType::None,
            context_label: "test",
        };
        assert!(BinaryStore::build(&[], &[], &[], config).is_ok());
    }

    #[test]
    fn parse_item_index_reads_correct_entries() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&5u64.to_le_bytes());
        raw.extend_from_slice(&3u64.to_le_bytes());
        raw.extend_from_slice(&8u64.to_le_bytes());
        raw.extend_from_slice(&1u64.to_le_bytes());

        let entries = BinaryStore::parse_item_index(&raw, 2).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].arrayref_start, 5);
        assert_eq!(entries[0].arrayref_count, 3);
        assert_eq!(entries[1].arrayref_start, 8);
        assert_eq!(entries[1].arrayref_count, 1);
    }

    #[test]
    fn dtype_to_stride_and_type_maps_all_known_codes() {
        assert!(matches!(
            BinaryStore::dtype_to_stride_and_type(1),
            Ok((8, NumericType::Float64))
        ));
        assert!(matches!(
            BinaryStore::dtype_to_stride_and_type(2),
            Ok((4, NumericType::Float32))
        ));
        assert!(matches!(
            BinaryStore::dtype_to_stride_and_type(3),
            Ok((2, NumericType::Float16))
        ));
        assert!(matches!(
            BinaryStore::dtype_to_stride_and_type(4),
            Ok((2, NumericType::Int16))
        ));
        assert!(matches!(
            BinaryStore::dtype_to_stride_and_type(5),
            Ok((4, NumericType::Int32))
        ));
        assert!(matches!(
            BinaryStore::dtype_to_stride_and_type(6),
            Ok((8, NumericType::Int64))
        ));
        assert!(BinaryStore::dtype_to_stride_and_type(99).is_err());
    }

    #[test]
    fn unshuffle2_inverts_shuffle2_output() {
        let original = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let shuffled = {
            let half = original.len() / 2;
            let mut s = vec![0u8; original.len()];
            for i in 0..half {
                s[i] = original[i * 2];
                s[i + half] = original[i * 2 + 1];
            }
            s
        };
        let mut recovered = vec![0u8; original.len()];
        unshuffle2(&shuffled, &mut recovered);
        assert_eq!(recovered, original);
    }

    #[test]
    fn unshuffle4_inverts_shuffle4_output() {
        let original: Vec<u8> = (0u8..16).collect();
        let quarter = original.len() / 4;
        let mut shuffled = vec![0u8; original.len()];
        for i in 0..quarter {
            let o = i * 4;
            shuffled[i] = original[o];
            shuffled[i + quarter] = original[o + 1];
            shuffled[i + 2 * quarter] = original[o + 2];
            shuffled[i + 3 * quarter] = original[o + 3];
        }
        let mut recovered = vec![0u8; original.len()];
        unshuffle4(&shuffled, &mut recovered);
        assert_eq!(recovered, original);
    }

    #[test]
    fn byte_cast_produces_correct_f32_values() {
        let values = [1.0f32, 2.0f32, 3.0f32];
        let bytes =
            unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, values.len() * 4) };
        let recovered: Vec<f32> = byte_cast(bytes);
        assert_eq!(recovered.len(), 3);
        assert!((recovered[0] - 1.0f32).abs() < f32::EPSILON);
        assert!((recovered[1] - 2.0f32).abs() < f32::EPSILON);
        assert!((recovered[2] - 3.0f32).abs() < f32::EPSILON);
    }
}
