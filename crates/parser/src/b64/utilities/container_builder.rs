use std::{collections::BTreeMap, io::Cursor};
use zstd::{bulk::Compressor, zstd_safe::compress_bound};

pub const BLOCK_DIR_ENTRY_SIZE: usize = 32;

#[derive(Clone, Copy)]
pub struct BlockDirEntry {
    pub payload_offset: u64,
    pub payload_size: u64,
    pub uncompressed_len_bytes: u64,
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
