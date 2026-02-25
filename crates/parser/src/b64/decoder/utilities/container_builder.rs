use zstd::{bulk::Compressor as ZstdCompressor, zstd_safe::compress_bound};

pub(crate) const BLOCK_DIRECTORY_ENTRY_SIZE: usize = 32;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FilterType {
    None = 0,
    Shuffle = 1,
}

impl TryFrom<u8> for FilterType {
    type Error = String;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Shuffle),
            _ => Err(format!("Unknown filter type: {value}")),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stride {
    S1 = 1,
    S2 = 2,
    S4 = 4,
    S8 = 8,
}

impl Stride {
    #[inline]
    pub(crate) fn from_size(size: usize) -> Self {
        match size {
            2 => Self::S2,
            4 => Self::S4,
            8 => Self::S8,
            _ => Self::S1,
        }
    }

    #[inline]
    pub(crate) fn as_usize(self) -> usize {
        self as usize
    }

    #[inline]
    pub(crate) fn array_idx(self) -> usize {
        match self {
            Self::S1 => 0,
            Self::S2 => 1,
            Self::S4 => 2,
            Self::S8 => 3,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct BlockDirEntry {
    pub(crate) payload_offset: u64,
    pub(crate) payload_size: u64,
    pub(crate) uncompressed_len_bytes: u64,
}

pub(crate) trait BlockCompressor {
    fn compress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<usize, String>;
    fn shuffle(&self, input: &[u8], output: &mut [u8], stride: usize);
    fn requires_shuffle(&self, filter_type: FilterType) -> bool;
}

pub(crate) struct DefaultCompressor {
    internal_compressor: ZstdCompressor<'static>,
}

impl DefaultCompressor {
    pub(crate) fn new(compression_level: i32) -> Result<Self, String> {
        Ok(Self {
            internal_compressor: ZstdCompressor::new(compression_level)
                .map_err(|error| error.to_string())?,
        })
    }
}

impl BlockCompressor for DefaultCompressor {
    fn compress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<usize, String> {
        output.clear();
        output.reserve(compress_bound(input.len()));
        self.internal_compressor
            .compress_to_buffer(input, output)
            .map_err(|error| error.to_string())
    }

    fn shuffle(&self, input: &[u8], output: &mut [u8], stride: usize) {
        shuffle_bytes(input, output, stride);
    }

    fn requires_shuffle(&self, filter_type: FilterType) -> bool {
        filter_type == FilterType::Shuffle
    }
}

pub(crate) enum CompressionMode<C: BlockCompressor> {
    Raw,
    Compressed(C),
}

pub(crate) enum BlockState {
    Active(u32),
    Sealed,
    Empty,
}

impl Default for BlockState {
    fn default() -> Self {
        BlockState::Empty
    }
}

#[derive(Default)]
struct PendingBlock {
    state: BlockState,
    data_buffer: Vec<u8>,
}

pub(crate) struct ContainerBuilder<C: BlockCompressor> {
    max_block_uncompressed_size: usize,
    filter_type: FilterType,
    active_blocks: [Option<PendingBlock>; 4],
    directory_entries: Vec<BlockDirEntry>,
    compressed_payload_accumulator: Vec<u8>,
    shuffling_scratch_pad: Vec<u8>,
    compression_scratch_pad: Vec<u8>,
    compressor_service: CompressionMode<C>,
}

impl<C: BlockCompressor> ContainerBuilder<C> {
    #[inline]
    pub(crate) fn new(
        max_block_uncompressed_size: usize,
        compressor_service: CompressionMode<C>,
        filter_type: FilterType,
    ) -> Self {
        Self {
            max_block_uncompressed_size,
            filter_type,
            active_blocks: [None, None, None, None],
            directory_entries: Vec::new(),
            compressed_payload_accumulator: Vec::new(),
            shuffling_scratch_pad: Vec::new(),
            compression_scratch_pad: Vec::new(),
            compressor_service,
        }
    }

    #[inline]
    pub(crate) fn add_item_to_box<F>(
        &mut self,
        item_byte_size: usize,
        stride: usize,
        write_action: F,
    ) -> Result<(u32, u64), String>
    where
        F: FnOnce(&mut Vec<u8>),
    {
        let stride = Stride::from_size(stride.max(1));
        if item_byte_size > self.max_block_uncompressed_size {
            self.add_oversized_item(item_byte_size, stride, write_action)
        } else {
            self.add_normal_item(item_byte_size, stride, write_action)
        }
    }

    #[inline]
    fn add_oversized_item<F>(
        &mut self,
        item_byte_size: usize,
        stride: Stride,
        write_action: F,
    ) -> Result<(u32, u64), String>
    where
        F: FnOnce(&mut Vec<u8>),
    {
        self.seal_block_by_stride(stride)?;

        let block_id = self.initialize_new_block(stride);
        let block = self.active_blocks[stride.array_idx()].as_mut().unwrap();

        block.data_buffer.reserve(item_byte_size);
        write_action(&mut block.data_buffer);

        self.seal_block_by_stride(stride)?;

        Ok((block_id, 0))
    }

    #[inline]
    fn add_normal_item<F>(
        &mut self,
        item_byte_size: usize,
        stride: Stride,
        write_action: F,
    ) -> Result<(u32, u64), String>
    where
        F: FnOnce(&mut Vec<u8>),
    {
        self.ensure_capacity_for_stride(item_byte_size, stride)?;

        let block_id = match &self.active_blocks[stride.array_idx()] {
            Some(block) => match block.state {
                BlockState::Active(id) => id,
                _ => self.initialize_new_block(stride),
            },
            None => self.initialize_new_block(stride),
        };

        let block = self.active_blocks[stride.array_idx()].as_mut().unwrap();
        let item_offset = (block.data_buffer.len() / stride.as_usize()) as u64;

        block.data_buffer.reserve(item_byte_size);
        write_action(&mut block.data_buffer);

        Ok((block_id, item_offset))
    }

    #[inline]
    fn initialize_new_block(&mut self, stride: Stride) -> u32 {
        let block_id = self.directory_entries.len() as u32;
        self.directory_entries.push(BlockDirEntry::default());

        let block =
            self.active_blocks[stride.array_idx()].get_or_insert_with(PendingBlock::default);
        block.state = BlockState::Active(block_id);
        block_id
    }

    #[inline]
    fn ensure_capacity_for_stride(
        &mut self,
        item_byte_size: usize,
        stride: Stride,
    ) -> Result<(), String> {
        if let Some(block) = &self.active_blocks[stride.array_idx()] {
            let would_exceed_limit = !block.data_buffer.is_empty()
                && block.data_buffer.len() + item_byte_size > self.max_block_uncompressed_size;

            if would_exceed_limit {
                return self.seal_block_by_stride(stride);
            }
        }
        Ok(())
    }

    #[inline]
    fn take_ready_block(&mut self, stride: Stride) -> Option<(u32, Vec<u8>)> {
        let block = self.active_blocks[stride.array_idx()].as_mut()?;
        if let BlockState::Active(block_id) = block.state {
            if !block.data_buffer.is_empty() {
                let data = std::mem::take(&mut block.data_buffer);
                block.state = BlockState::Sealed;
                return Some((block_id, data));
            }
        }
        None
    }

    #[inline]
    fn seal_block_by_stride(&mut self, stride: Stride) -> Result<(), String> {
        let (block_id, data) = match self.take_ready_block(stride) {
            Some(ready) => ready,
            None => return Ok(()),
        };

        let payload_offset = self.compressed_payload_accumulator.len() as u64;
        let uncompressed_size = data.len() as u64;

        match &mut self.compressor_service {
            CompressionMode::Compressed(service) => {
                let needs_shuffle =
                    service.requires_shuffle(self.filter_type) && stride != Stride::S1;
                let source = if needs_shuffle {
                    self.shuffling_scratch_pad.resize(data.len(), 0);
                    service.shuffle(&data, &mut self.shuffling_scratch_pad, stride.as_usize());
                    &self.shuffling_scratch_pad
                } else {
                    &data
                };
                service.compress(source, &mut self.compression_scratch_pad)?;
                self.compressed_payload_accumulator
                    .extend_from_slice(&self.compression_scratch_pad);
            }
            CompressionMode::Raw => {
                self.compressed_payload_accumulator.extend_from_slice(&data);
            }
        }

        let payload_size = self.compressed_payload_accumulator.len() as u64 - payload_offset;

        self.directory_entries[block_id as usize] = BlockDirEntry {
            payload_offset,
            payload_size,
            uncompressed_len_bytes: uncompressed_size,
        };

        Ok(())
    }

    #[inline]
    pub(crate) fn pack(mut self) -> Result<(Vec<u8>, u32), String> {
        for stride in [Stride::S1, Stride::S2, Stride::S4, Stride::S8] {
            self.seal_block_by_stride(stride)?;
        }

        let total_blocks = self.directory_entries.len() as u32;
        let directory_byte_size = self.directory_entries.len() * BLOCK_DIRECTORY_ENTRY_SIZE;
        let mut final_container =
            Vec::with_capacity(directory_byte_size + self.compressed_payload_accumulator.len());

        for entry in &self.directory_entries {
            final_container.extend_from_slice(&entry.payload_offset.to_le_bytes());
            final_container.extend_from_slice(&entry.payload_size.to_le_bytes());
            final_container.extend_from_slice(&entry.uncompressed_len_bytes.to_le_bytes());
            final_container.extend_from_slice(&[0u8; 8]);
        }

        final_container.extend_from_slice(&self.compressed_payload_accumulator);
        Ok((final_container, total_blocks))
    }
}

#[inline(always)]
fn shuffle_bytes(input: &[u8], output: &mut [u8], stride: usize) {
    match stride {
        8 => shuffle8(input, output),
        4 => shuffle4(input, output),
        2 => shuffle2(input, output),
        _ => shuffle_any(input, output, stride),
    }
}

#[inline(always)]
fn shuffle2(input: &[u8], output: &mut [u8]) {
    let half_length = input.len() / 2;
    let (first_half, second_half) = output.split_at_mut(half_length);
    for index in 0..half_length {
        let input_offset = index * 2;
        first_half[index] = input[input_offset];
        second_half[index] = input[input_offset + 1];
    }
}

#[inline(always)]
fn shuffle4(input: &[u8], output: &mut [u8]) {
    let quarter_length = input.len() / 4;
    let (byte_0_group, remainder) = output.split_at_mut(quarter_length);
    let (byte_1_group, remainder) = remainder.split_at_mut(quarter_length);
    let (byte_2_group, byte_3_group) = remainder.split_at_mut(quarter_length);
    for index in 0..quarter_length {
        let input_offset = index * 4;
        byte_0_group[index] = input[input_offset];
        byte_1_group[index] = input[input_offset + 1];
        byte_2_group[index] = input[input_offset + 2];
        byte_3_group[index] = input[input_offset + 3];
    }
}

#[inline(always)]
fn shuffle8(input: &[u8], output: &mut [u8]) {
    let segment_length = input.len() / 8;
    let (byte_0_group, remainder) = output.split_at_mut(segment_length);
    let (byte_1_group, remainder) = remainder.split_at_mut(segment_length);
    let (byte_2_group, remainder) = remainder.split_at_mut(segment_length);
    let (byte_3_group, remainder) = remainder.split_at_mut(segment_length);
    let (byte_4_group, remainder) = remainder.split_at_mut(segment_length);
    let (byte_5_group, remainder) = remainder.split_at_mut(segment_length);
    let (byte_6_group, byte_7_group) = remainder.split_at_mut(segment_length);
    for index in 0..segment_length {
        let input_offset = index * 8;
        byte_0_group[index] = input[input_offset];
        byte_1_group[index] = input[input_offset + 1];
        byte_2_group[index] = input[input_offset + 2];
        byte_3_group[index] = input[input_offset + 3];
        byte_4_group[index] = input[input_offset + 4];
        byte_5_group[index] = input[input_offset + 5];
        byte_6_group[index] = input[input_offset + 6];
        byte_7_group[index] = input[input_offset + 7];
    }
}

#[inline(always)]
fn shuffle_any(input: &[u8], output: &mut [u8], stride: usize) {
    let element_count = input.len() / stride;
    for byte_position in 0..stride {
        let output_base_offset = byte_position * element_count;
        for element_index in 0..element_count {
            output[output_base_offset + element_index] =
                input[byte_position + element_index * stride];
        }
    }
}
