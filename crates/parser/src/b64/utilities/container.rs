use crate::b64::utilities::common::{decompress_zstd, take};

use std::{collections::BTreeMap, io::Cursor};
use zstd::{bulk::Compressor, zstd_safe::compress_bound};

const BLOCK_DIR_ENTRY_SIZE: usize = 32;
const ARRAY_FILTER_BYTE_SHUFFLE: u8 = 1;

#[derive(Clone, Copy)]
pub struct BlockDirEntry {
    pub payload_offset: u64,
    pub payload_size: u64,
    pub uncompressed_len_bytes: u64,
}

enum CachedBlockBytes<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> CachedBlockBytes<'a> {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            CachedBlockBytes::Borrowed(b) => b,
            CachedBlockBytes::Owned(v) => v.as_slice(),
        }
    }
}

struct BlockBox {
    block_index: Option<u32>,
    buffer: Vec<u8>,
}

pub struct ContainerBuilder {
    target_block_uncomp_byte_size: usize,
    compression_level: u8,
    do_shuffle: bool,
    boxes: BTreeMap<usize, BlockBox>,
    entries: Vec<BlockDirEntry>,
    compressed: Vec<u8>,
    scratch: Vec<u8>,
    compressor: Option<Compressor<'static>>,
}

impl ContainerBuilder {
    #[inline]
    pub fn new(
        target_block_uncomp_byte_size: usize,
        compression_level: u8,
        do_shuffle: bool,
    ) -> Self {
        let compressor = if compression_level == 0 {
            None
        } else {
            Some(Compressor::new(compression_level as i32).unwrap())
        };

        Self {
            target_block_uncomp_byte_size,
            compression_level,
            do_shuffle,
            boxes: BTreeMap::new(),
            entries: Vec::new(),
            compressed: Vec::new(),
            scratch: Vec::new(),
            compressor,
        }
    }

    #[inline]
    pub fn add_item_to_box<F>(
        &mut self,
        item_bytes: usize,
        elem_size: usize,
        write_fn: F,
    ) -> (u32, u64)
    where
        F: FnOnce(&mut Vec<u8>),
    {
        let element_size = elem_size.max(1);

        if item_bytes > self.target_block_uncomp_byte_size {
            self.seal_box(element_size);

            let b = self.boxes.entry(element_size).or_insert_with(|| BlockBox {
                block_index: None,
                buffer: Vec::new(),
            });

            let block_idx = self.entries.len() as u32;
            self.entries.push(BlockDirEntry {
                payload_offset: 0,
                payload_size: 0,
                uncompressed_len_bytes: 0,
            });
            b.block_index = Some(block_idx);

            b.buffer.reserve(item_bytes);
            write_fn(&mut b.buffer);

            self.seal_box(element_size);
            return (block_idx, 0);
        }

        self.ensure_box_has_space(item_bytes, element_size);
        let (index, element_off) = {
            let block_box = self.boxes.entry(element_size).or_insert_with(|| BlockBox {
                block_index: None,
                buffer: Vec::new(),
            });

            let box_index = match block_box.block_index {
                Some(idx) => idx,
                None => {
                    let idx = self.entries.len() as u32;
                    self.entries.push(BlockDirEntry {
                        payload_offset: 0,
                        payload_size: 0,
                        uncompressed_len_bytes: 0,
                    });
                    block_box.block_index = Some(idx);
                    idx
                }
            };

            let offset = (block_box.buffer.len() / element_size) as u64;

            block_box.buffer.reserve(item_bytes);
            write_fn(&mut block_box.buffer);

            (box_index, offset)
        };

        (index, element_off)
    }

    #[inline]
    fn seal_box(&mut self, element_size_bytes: usize) {
        let open_box = match self.boxes.get_mut(&element_size_bytes) {
            Some(open_box) => open_box,
            None => return,
        };

        let block_index = match open_box.block_index {
            Some(id) => id,
            None => return,
        };

        if open_box.buffer.is_empty() {
            open_box.block_index = None;
            return;
        }

        let uncompressed_len_bytes = open_box.buffer.len() as u64;
        let payload_offset = self.compressed.len() as u64;

        if self.compression_level == 0 {
            self.entries[block_index as usize] = BlockDirEntry {
                payload_offset,
                payload_size: uncompressed_len_bytes,
                uncompressed_len_bytes,
            };
            self.compressed.extend_from_slice(&open_box.buffer);
            open_box.buffer.clear();
            open_box.block_index = None;
            return;
        }

        let element_size = element_size_bytes.max(1);

        let uncompressed: &[u8] = if self.do_shuffle && element_size > 1 {
            let needed = open_box.buffer.len();
            if self.scratch.len() < needed {
                self.scratch.resize(needed, 0);
            }
            let scratch = &mut self.scratch[..needed];

            byte_shuffle_into(open_box.buffer.as_slice(), scratch, element_size);
            scratch
        } else {
            open_box.buffer.as_slice()
        };

        let compressor = self.compressor.as_mut().unwrap();

        let max_compressed_size = compress_bound(uncompressed.len());
        self.compressed.reserve(max_compressed_size);

        let mut compressed_payload_writer = Cursor::new(&mut self.compressed);
        compressed_payload_writer.set_position(payload_offset);

        let compressed_size = compressor
            .compress_to_buffer(uncompressed, &mut compressed_payload_writer)
            .unwrap() as u64;

        self.entries[block_index as usize] = BlockDirEntry {
            payload_offset,
            payload_size: compressed_size,
            uncompressed_len_bytes,
        };

        open_box.buffer.clear();
        open_box.block_index = None;
    }

    #[inline]
    fn ensure_box_has_space(&mut self, item_bytes: usize, element_size_bytes: usize) {
        let element_size_bytes = element_size_bytes.max(1);

        // Check if the current open box for this element size would overflow if we add this item.
        let should_seal_current_box = self
            .boxes
            .get(&element_size_bytes)
            .map(|block_box| {
                !block_box.buffer.is_empty()
                    && block_box.buffer.len() + item_bytes > self.target_block_uncomp_byte_size
            })
            .unwrap_or(false);

        if should_seal_current_box {
            self.seal_box(element_size_bytes);
        }
    }

    #[inline]
    pub fn pack(mut self) -> (Vec<u8>, u32) {
        let mut open: Vec<(u32, usize)> = self
            .boxes
            .iter()
            .filter_map(|(&element_size_bytes, block_box)| {
                block_box
                    .block_index
                    .map(|block_index| (block_index, element_size_bytes))
            })
            .collect();

        open.sort_unstable_by_key(|(block_index, _)| *block_index);

        for (_, element_size_bytes) in open {
            self.seal_box(element_size_bytes);
        }

        let block_count = self.entries.len() as u32;
        let directory_byte_size = self.entries.len() * BLOCK_DIR_ENTRY_SIZE;

        let payload_len = self.compressed.len();
        self.compressed.reserve(directory_byte_size);
        self.compressed.resize(payload_len + directory_byte_size, 0);
        self.compressed
            .copy_within(0..payload_len, directory_byte_size);

        let mut at = 0usize;
        for block in &self.entries {
            self.compressed[at..at + 8].copy_from_slice(&block.payload_offset.to_le_bytes());
            at += 8;
            self.compressed[at..at + 8].copy_from_slice(&block.payload_size.to_le_bytes());
            at += 8;
            self.compressed[at..at + 8]
                .copy_from_slice(&block.uncompressed_len_bytes.to_le_bytes());
            at += 8;
            self.compressed[at..at + 8].fill(0);
            at += 8;
        }

        (self.compressed, block_count)
    }
}

pub struct ContainerView<'a> {
    container: &'a [u8],
    directory_byte_size: usize,
    entries: Vec<BlockDirEntry>,
    block_cache: Vec<Option<CachedBlockBytes<'a>>>,
    scratch: Vec<u8>,
    block_elem_sizes: Vec<usize>,
    compression_level: u8,
    array_filter: u8,
}

impl<'a> ContainerView<'a> {
    #[inline]
    pub fn new(
        container: &'a [u8],
        block_count: u32,
        compression_level: u8,
        array_filter: u8,
        field: &'static str,
    ) -> Result<Self, String> {
        let block_count = block_count as usize;

        let directory_byte_size = block_count
            .checked_mul(BLOCK_DIR_ENTRY_SIZE)
            .ok_or_else(|| format!("{field}: dir size overflow"))?;

        if container.len() < directory_byte_size {
            return Err(format!("{field}: too small for directory"));
        }

        let directory = &container[..directory_byte_size];
        let mut pos = 0usize;

        let mut entries = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            let payload_offset = read_u64_le_at(directory, &mut pos, "payload_offset")?;
            let payload_size = read_u64_le_at(directory, &mut pos, "payload_size")?;
            let uncompressed_len_bytes =
                read_u64_le_at(directory, &mut pos, "uncompressed_len_bytes")?;
            let _ = take(directory, &mut pos, 8, "reserved")?;

            entries.push(BlockDirEntry {
                payload_offset,
                payload_size,
                uncompressed_len_bytes,
            });
        }

        let mut block_cache: Vec<Option<CachedBlockBytes<'a>>> = Vec::with_capacity(block_count);
        block_cache.resize_with(block_count, || None);

        Ok(Self {
            container,
            directory_byte_size,
            entries,
            block_cache,
            scratch: Vec::new(),
            block_elem_sizes: vec![0; block_count],
            compression_level,
            array_filter,
        })
    }

    #[inline]
    pub fn ensure_block_loaded(
        &mut self,
        idx: u32,
        element_size_bytes: usize,
        field: &'static str,
    ) -> Result<(), String> {
        let block_index = idx as usize;
        if block_index >= self.block_cache.len() {
            return Err(format!("{field}: block index out of range: {idx}"));
        }

        if self.block_cache[block_index].is_some() {
            return Ok(());
        }

        let element_size_bytes = element_size_bytes.max(1);
        let needs_unshuffle =
            self.array_filter == ARRAY_FILTER_BYTE_SHUFFLE && element_size_bytes > 1;

        if needs_unshuffle {
            let prev = self.block_elem_sizes[block_index];
            if prev == 0 {
                self.block_elem_sizes[block_index] = element_size_bytes;
            } else if prev != element_size_bytes {
                return Err(format!(
                    "{field}: block elem_size mismatch for block index={idx} (prev={prev}, now={element_size_bytes})"
                ));
            }
        }

        let entry = self.entries[block_index];

        let payload_offset = usize::try_from(entry.payload_offset)
            .map_err(|_| format!("{field}: payload_offset overflow"))?;
        let payload_size = usize::try_from(entry.payload_size)
            .map_err(|_| format!("{field}: payload_size overflow"))?;
        let expected_uncompressed_len = usize::try_from(entry.uncompressed_len_bytes)
            .map_err(|_| format!("{field}: uncompressed_len_bytes overflow"))?;

        let block_start = self
            .directory_byte_size
            .checked_add(payload_offset)
            .ok_or_else(|| format!("{field}: payload start overflow"))?;

        let block_end = block_start
            .checked_add(payload_size)
            .ok_or_else(|| format!("{field}: payload end overflow"))?;

        if block_end > self.container.len() {
            return Err(format!("{field}: block range out of bounds"));
        }

        let stored_payload = &self.container[block_start..block_end];

        if self.compression_level == 0 && !needs_unshuffle {
            if stored_payload.len() != expected_uncompressed_len {
                return Err(format!(
                    "{field}: bad block size (block index={idx}, got={}, expected={})",
                    stored_payload.len(),
                    expected_uncompressed_len
                ));
            }
            self.block_cache[block_index] = Some(CachedBlockBytes::Borrowed(stored_payload));
            return Ok(());
        }

        let mut uncompressed_block = if self.compression_level == 0 {
            stored_payload.to_vec()
        } else {
            decompress_zstd(stored_payload, expected_uncompressed_len)?
        };

        if uncompressed_block.len() != expected_uncompressed_len {
            return Err(format!(
                "{field}: bad block size (block index={idx}, got={}, expected={})",
                uncompressed_block.len(),
                expected_uncompressed_len
            ));
        }

        if needs_unshuffle {
            self.scratch.resize(uncompressed_block.len(), 0);
            byte_unshuffle_into(
                uncompressed_block.as_slice(),
                self.scratch.as_mut_slice(),
                element_size_bytes,
            );
            std::mem::swap(&mut uncompressed_block, &mut self.scratch);
        }

        self.block_cache[block_index] = Some(CachedBlockBytes::Owned(uncompressed_block));
        Ok(())
    }

    #[inline]
    pub fn get_block_bytes(
        &mut self,
        idx: u32,
        element_size_bytes: usize,
        field: &'static str,
    ) -> Result<&[u8], String> {
        self.ensure_block_loaded(idx, element_size_bytes, field)?;
        Ok(self.block_cache[idx as usize].as_ref().unwrap().as_slice())
    }

    #[inline]
    pub fn get_item_from_block(
        &mut self,
        idx: u32,
        element_off: u64,
        len_elements: u64,
        element_size_bytes: usize,
        field: &'static str,
    ) -> Result<&[u8], String> {
        self.ensure_block_loaded(idx, element_size_bytes, field)?;

        let element_size_bytes = element_size_bytes.max(1);

        let element_off =
            usize::try_from(element_off).map_err(|_| format!("{field}: element_off overflow"))?;
        let len_elements =
            usize::try_from(len_elements).map_err(|_| format!("{field}: len_elements overflow"))?;

        let byte_off = element_off
            .checked_mul(element_size_bytes)
            .ok_or_else(|| format!("{field}: byte_off overflow"))?;
        let byte_len = len_elements
            .checked_mul(element_size_bytes)
            .ok_or_else(|| format!("{field}: byte_len overflow"))?;

        let byte_end = byte_off
            .checked_add(byte_len)
            .ok_or_else(|| format!("{field}: slice end overflow"))?;

        let block_bytes = self.block_cache[idx as usize].as_ref().unwrap().as_slice();

        if byte_end > block_bytes.len() {
            return Err(format!("{field}: item slice out of bounds"));
        }

        Ok(&block_bytes[byte_off..byte_end])
    }
}

#[inline(always)]
fn byte_unshuffle_into(input: &[u8], output: &mut [u8], elem_size: usize) {
    assert_eq!(input.len(), output.len(), "in/out size mismatch");
    assert_eq!(input.len() % elem_size, 0, "len not multiple of elem_size");

    match elem_size {
        4 => unshuffle4(input, output),
        8 => unshuffle8(input, output),
        _ => unshuffle_generic(input, output, elem_size),
    }
}

#[inline(always)]
fn byte_shuffle_into(input: &[u8], output: &mut [u8], elem_size: usize) {
    assert_eq!(input.len(), output.len(), "in/out size mismatch");
    assert_eq!(input.len() % elem_size, 0, "len not multiple of elem_size");

    match elem_size {
        4 => shuffle4(input, output),
        8 => shuffle8(input, output),
        _ => shuffle_generic(input, output, elem_size),
    }
}

#[inline(always)]
fn unshuffle4(input: &[u8], output: &mut [u8]) {
    let n = input.len() / 4;

    let (b0, rest) = input.split_at(n);
    let (b1, rest) = rest.split_at(n);
    let (b2, b3) = rest.split_at(n);

    for i in 0..n {
        let o = i * 4;
        output[o] = b0[i];
        output[o + 1] = b1[i];
        output[o + 2] = b2[i];
        output[o + 3] = b3[i];
    }
}

#[inline(always)]
fn unshuffle8(input: &[u8], output: &mut [u8]) {
    let n = input.len() / 8;

    let (b0, rest) = input.split_at(n);
    let (b1, rest) = rest.split_at(n);
    let (b2, rest) = rest.split_at(n);
    let (b3, rest) = rest.split_at(n);
    let (b4, rest) = rest.split_at(n);
    let (b5, rest) = rest.split_at(n);
    let (b6, b7) = rest.split_at(n);

    for i in 0..n {
        let o = i * 8;
        output[o] = b0[i];
        output[o + 1] = b1[i];
        output[o + 2] = b2[i];
        output[o + 3] = b3[i];
        output[o + 4] = b4[i];
        output[o + 5] = b5[i];
        output[o + 6] = b6[i];
        output[o + 7] = b7[i];
    }
}

#[inline(always)]
fn unshuffle_generic(input: &[u8], output: &mut [u8], elem_size: usize) {
    let count = input.len() / elem_size;
    for b in 0..elem_size {
        let in_base = b * count;
        for e in 0..count {
            output[b + e * elem_size] = input[in_base + e];
        }
    }
}

#[inline(always)]
fn shuffle4(input: &[u8], output: &mut [u8]) {
    let n = input.len() / 4;

    let (b0, rest) = output.split_at_mut(n);
    let (b1, rest) = rest.split_at_mut(n);
    let (b2, b3) = rest.split_at_mut(n);

    for i in 0..n {
        let o = i * 4;
        b0[i] = input[o];
        b1[i] = input[o + 1];
        b2[i] = input[o + 2];
        b3[i] = input[o + 3];
    }
}

#[inline(always)]
fn shuffle8(input: &[u8], output: &mut [u8]) {
    let n = input.len() / 8;

    let (b0, rest) = output.split_at_mut(n);
    let (b1, rest) = rest.split_at_mut(n);
    let (b2, rest) = rest.split_at_mut(n);
    let (b3, rest) = rest.split_at_mut(n);
    let (b4, rest) = rest.split_at_mut(n);
    let (b5, rest) = rest.split_at_mut(n);
    let (b6, b7) = rest.split_at_mut(n);

    for i in 0..n {
        let o = i * 8;
        b0[i] = input[o];
        b1[i] = input[o + 1];
        b2[i] = input[o + 2];
        b3[i] = input[o + 3];
        b4[i] = input[o + 4];
        b5[i] = input[o + 5];
        b6[i] = input[o + 6];
        b7[i] = input[o + 7];
    }
}

#[inline(always)]
fn shuffle_generic(input: &[u8], output: &mut [u8], elem_size: usize) {
    let count = input.len() / elem_size;
    for b in 0..elem_size {
        let out_base = b * count;
        for e in 0..count {
            output[out_base + e] = input[b + e * elem_size];
        }
    }
}

#[inline]
pub fn read_u64_le_at(bytes: &[u8], pos: &mut usize, field: &'static str) -> Result<u64, String> {
    let s = take(bytes, pos, 8, field)?;
    Ok(u64::from_le_bytes(s.try_into().unwrap()))
}
