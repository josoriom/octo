use crate::b64::utilities::common::{decompress_zstd, read_u64_le_at, take};
use crate::b64::utilities::container_builder::{
    BLOCK_DIRECTORY_ENTRY_SIZE, BlockDirEntry, FilterType, Stride,
};
use std::ops::Deref;

pub(crate) trait BlockProcessor {
    fn decompress(&self, source: &[u8], target_len: usize) -> Result<Vec<u8>, String>;
    fn unshuffle(&self, source: &[u8], target: &mut [u8], stride: usize);
    fn requires_unshuffle(&self, filter: FilterType) -> bool;
}

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

pub(crate) struct ContainerView<'a, P: BlockProcessor> {
    raw_data: &'a [u8],
    header_size: usize,
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
        let header_size = block_count * BLOCK_DIRECTORY_ENTRY_SIZE;

        if raw_data.len() < header_size {
            return Err(format!("{ctx}: directory truncated"));
        }

        let header = &raw_data[..header_size];
        let mut entries = Vec::with_capacity(block_count);
        let mut offset = 0;

        for _ in 0..block_count {
            let payload_offset = read_u64_le_at(header, &mut offset, ctx)?;
            let payload_size = read_u64_le_at(header, &mut offset, ctx)?;
            let uncompressed_len_bytes = read_u64_le_at(header, &mut offset, ctx)?;
            let _padding = take(header, &mut offset, 8, ctx)?;

            entries.push(BlockDirEntry {
                payload_offset,
                payload_size,
                uncompressed_len_bytes,
            });
        }

        let mut cache = Vec::with_capacity(block_count);
        cache.resize_with(block_count, || None);

        let stride_history = vec![None; block_count];

        Ok(Self {
            raw_data,
            header_size,
            entries,
            cache,
            scratch_buffer: Vec::new(),
            stride_history,
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
        stride: usize,
        ctx: &'static str,
    ) -> Result<&[u8], String> {
        self.ensure_block_loaded(block_id, stride, ctx)?;

        let block = self.cache[block_id as usize].as_ref().unwrap();
        let start_byte = (element_offset as usize) * stride;
        let end_byte = start_byte + (element_count as usize) * stride;

        if end_byte > block.len() {
            return Err(format!(
                "{ctx}: item range out of bounds for block {block_id}"
            ));
        }
        Ok(&block[start_byte..end_byte])
    }

    pub(crate) fn ensure_block_loaded(
        &mut self,
        block_id: u32,
        stride_size: usize,
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

        let stride = Stride::from_size(stride_size);
        self.validate_stride(block_index, stride, ctx)?;

        let entry = self.entries[block_index];
        let payload_start = self.header_size + entry.payload_offset as usize;
        let payload_end = payload_start + entry.payload_size as usize;

        if payload_end > self.raw_data.len() {
            return Err(format!(
                "{ctx}: block {block_index} payload exceeds data bounds"
            ));
        }

        let decoded = self.decode_pipeline(
            &self.raw_data[payload_start..payload_end],
            entry.uncompressed_len_bytes as usize,
            stride,
        )?;
        self.cache[block_index] = Some(decoded);
        Ok(())
    }

    fn validate_stride(
        &mut self,
        block_index: usize,
        stride: Stride,
        ctx: &'static str,
    ) -> Result<(), String> {
        if self.processor.requires_unshuffle(self.filter) && stride != Stride::S1 {
            match self.stride_history[block_index] {
                None => self.stride_history[block_index] = Some(stride),
                Some(recorded) if recorded != stride => {
                    return Err(format!("{ctx}: stride mismatch for block {block_index}"));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn decode_pipeline(
        &mut self,
        payload: &'a [u8],
        uncompressed_len: usize,
        stride: Stride,
    ) -> Result<BlockData<'a>, String> {
        let needs_unshuffle =
            self.processor.requires_unshuffle(self.filter) && stride != Stride::S1;

        if self.compression_level == 0 && !needs_unshuffle {
            if payload.len() != uncompressed_len {
                return Err("uncompressed payload size mismatch".into());
            }
            return Ok(BlockData::Borrowed(payload));
        }

        let mut data = if self.compression_level == 0 {
            payload.to_vec()
        } else {
            self.processor.decompress(payload, uncompressed_len)?
        };

        if needs_unshuffle {
            self.scratch_buffer.resize(uncompressed_len, 0);
            self.processor
                .unshuffle(&data, &mut self.scratch_buffer, stride.as_usize());
            std::mem::swap(&mut data, &mut self.scratch_buffer);
        }

        Ok(BlockData::Owned(data))
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
    let half_length = source.len() / 2;
    let (first_half, second_half) = source.split_at(half_length);
    for index in 0..half_length {
        let target_offset = index * 2;
        target[target_offset] = first_half[index];
        target[target_offset + 1] = second_half[index];
    }
}

#[inline(always)]
fn unshuffle4(source: &[u8], target: &mut [u8]) {
    let quarter_length = source.len() / 4;
    let (byte_0_group, remainder) = source.split_at(quarter_length);
    let (byte_1_group, remainder) = remainder.split_at(quarter_length);
    let (byte_2_group, byte_3_group) = remainder.split_at(quarter_length);
    for index in 0..quarter_length {
        let target_offset = index * 4;
        target[target_offset] = byte_0_group[index];
        target[target_offset + 1] = byte_1_group[index];
        target[target_offset + 2] = byte_2_group[index];
        target[target_offset + 3] = byte_3_group[index];
    }
}

#[inline(always)]
fn unshuffle8(source: &[u8], target: &mut [u8]) {
    let segment_length = source.len() / 8;
    let (byte_0_group, remainder) = source.split_at(segment_length);
    let (byte_1_group, remainder) = remainder.split_at(segment_length);
    let (byte_2_group, remainder) = remainder.split_at(segment_length);
    let (byte_3_group, remainder) = remainder.split_at(segment_length);
    let (byte_4_group, remainder) = remainder.split_at(segment_length);
    let (byte_5_group, remainder) = remainder.split_at(segment_length);
    let (byte_6_group, byte_7_group) = remainder.split_at(segment_length);
    for index in 0..segment_length {
        let target_offset = index * 8;
        target[target_offset] = byte_0_group[index];
        target[target_offset + 1] = byte_1_group[index];
        target[target_offset + 2] = byte_2_group[index];
        target[target_offset + 3] = byte_3_group[index];
        target[target_offset + 4] = byte_4_group[index];
        target[target_offset + 5] = byte_5_group[index];
        target[target_offset + 6] = byte_6_group[index];
        target[target_offset + 7] = byte_7_group[index];
    }
}

#[inline(always)]
fn unshuffle_any(source: &[u8], target: &mut [u8], stride: usize) {
    let element_count = source.len() / stride;
    for byte_position in 0..stride {
        let source_base_offset = byte_position * element_count;
        for element_index in 0..element_count {
            let target_index = byte_position + (element_index * stride);
            target[target_index] = source[source_base_offset + element_index];
        }
    }
}
