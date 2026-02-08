const HEADER_SIZE: usize = 512;
const RESERVED_SIZE: usize = HEADER_SIZE - 256;

pub fn parse_header(bytes: &[u8]) -> Result<Header, String> {
    if bytes.len() < HEADER_SIZE {
        return Err("header: file too small".to_string());
    }

    let mut r = Reader::new(&bytes[..HEADER_SIZE]);

    let file_signature = r.read_arr::<4>("file_signature")?;
    let endianness_flag = r.read_u8("endianness_flag")?;
    let reserved_alignment = r.read_arr::<3>("reserved_alignment")?;

    if &file_signature != b"B000" {
        return Err("header: invalid file_signature (expected \"B000\")".into());
    }
    if endianness_flag != 0 {
        return Err("header: expected little-endian endianness_flag=0".into());
    }

    let off_spec_entries = r.read_u64_le("off_spec_entries")?;
    let len_spec_entries = r.read_u64_le("len_spec_entries")?;

    let off_spec_arrayrefs = r.read_u64_le("off_spec_arrayrefs")?;
    let len_spec_arrayrefs = r.read_u64_le("len_spec_arrayrefs")?;

    let off_chrom_entries = r.read_u64_le("off_chrom_entries")?;
    let len_chrom_entries = r.read_u64_le("len_chrom_entries")?;

    let off_chrom_arrayrefs = r.read_u64_le("off_chrom_arrayrefs")?;
    let len_chrom_arrayrefs = r.read_u64_le("len_chrom_arrayrefs")?;

    let off_spec_meta = r.read_u64_le("off_spec_meta")?;
    let len_spec_meta = r.read_u64_le("len_spec_meta")?;

    let off_chrom_meta = r.read_u64_le("off_chrom_meta")?;
    let len_chrom_meta = r.read_u64_le("len_chrom_meta")?;

    let off_global_meta = r.read_u64_le("off_global_meta")?;
    let len_global_meta = r.read_u64_le("len_global_meta")?;

    let off_container_spect = r.read_u64_le("off_container_spect")?;
    let len_container_spect = r.read_u64_le("len_container_spect")?;

    let off_container_chrom = r.read_u64_le("off_container_chrom")?;
    let len_container_chrom = r.read_u64_le("len_container_chrom")?;

    let block_count_spect = r.read_u32_le("block_count_spect")?;
    let block_count_chrom = r.read_u32_le("block_count_chrom")?;

    let spectrum_count = r.read_u32_le("spectrum_count")?;
    let chrom_count = r.read_u32_le("chrom_count")?;

    let spec_meta_count = r.read_u32_le("spec_meta_count")?;
    let spec_num_count = r.read_u32_le("spec_num_count")?;
    let spec_str_count = r.read_u32_le("spec_str_count")?;

    let chrom_meta_count = r.read_u32_le("chrom_meta_count")?;
    let chrom_num_count = r.read_u32_le("chrom_num_count")?;
    let chrom_str_count = r.read_u32_le("chrom_str_count")?;

    let global_meta_count = r.read_u32_le("global_meta_count")?;
    let global_num_count = r.read_u32_le("global_num_count")?;
    let global_str_count = r.read_u32_le("global_str_count")?;

    let spect_array_type_count = r.read_u32_le("spect_array_type_count")?;
    let chrom_array_type_count = r.read_u32_le("chrom_array_type_count")?;

    let _pad_212_215 = r.read_arr::<4>("pad_212_215")?;

    let target_block_uncomp_bytes = r.read_u64_le("target_block_uncomp_bytes")?;

    let codec_id = r.read_u8("codec_id")?;
    let compression_level = r.read_u8("compression_level")?;
    let array_filter = r.read_u8("array_filter")?;

    let _pad_227_231 = r.read_arr::<5>("pad_227_231")?;

    let size_spec_meta_uncompressed = r.read_u64_le("size_spec_meta_uncompressed")?;
    let size_chrom_meta_uncompressed = r.read_u64_le("size_chrom_meta_uncompressed")?;
    let size_global_meta_uncompressed = r.read_u64_le("size_global_meta_uncompressed")?;

    let reserved = r.read_arr::<RESERVED_SIZE>("reserved")?;

    Ok(Header {
        file_signature,
        endianness_flag,
        reserved_alignment,

        off_spec_entries,
        len_spec_entries,
        off_spec_arrayrefs,
        len_spec_arrayrefs,

        off_chrom_entries,
        len_chrom_entries,
        off_chrom_arrayrefs,
        len_chrom_arrayrefs,

        off_spec_meta,
        len_spec_meta,
        off_chrom_meta,
        len_chrom_meta,
        off_global_meta,
        len_global_meta,

        off_container_spect,
        len_container_spect,
        off_container_chrom,
        len_container_chrom,

        block_count_spect,
        block_count_chrom,

        spectrum_count,
        chrom_count,

        spec_meta_count,
        spec_num_count,
        spec_str_count,

        chrom_meta_count,
        chrom_num_count,
        chrom_str_count,

        global_meta_count,
        global_num_count,
        global_str_count,

        spect_array_type_count,
        chrom_array_type_count,

        target_block_uncomp_bytes,

        codec_id,
        compression_level,
        array_filter,

        size_spec_meta_uncompressed,
        size_chrom_meta_uncompressed,
        size_global_meta_uncompressed,

        reserved,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub file_signature: [u8; 4],
    pub endianness_flag: u8,
    pub reserved_alignment: [u8; 3],

    pub off_spec_entries: u64,
    pub len_spec_entries: u64,
    pub off_spec_arrayrefs: u64,
    pub len_spec_arrayrefs: u64,

    pub off_chrom_entries: u64,
    pub len_chrom_entries: u64,
    pub off_chrom_arrayrefs: u64,
    pub len_chrom_arrayrefs: u64,

    pub off_spec_meta: u64,
    pub len_spec_meta: u64,
    pub off_chrom_meta: u64,
    pub len_chrom_meta: u64,
    pub off_global_meta: u64,
    pub len_global_meta: u64,

    pub off_container_spect: u64,
    pub len_container_spect: u64,
    pub off_container_chrom: u64,
    pub len_container_chrom: u64,

    pub block_count_spect: u32,
    pub block_count_chrom: u32,

    pub spectrum_count: u32,
    pub chrom_count: u32,

    pub spec_meta_count: u32,
    pub spec_num_count: u32,
    pub spec_str_count: u32,

    pub chrom_meta_count: u32,
    pub chrom_num_count: u32,
    pub chrom_str_count: u32,

    pub global_meta_count: u32,
    pub global_num_count: u32,
    pub global_str_count: u32,

    pub spect_array_type_count: u32,
    pub chrom_array_type_count: u32,

    pub target_block_uncomp_bytes: u64,

    pub codec_id: u8,
    pub compression_level: u8,
    pub array_filter: u8,

    pub size_spec_meta_uncompressed: u64,
    pub size_chrom_meta_uncompressed: u64,
    pub size_global_meta_uncompressed: u64,

    pub reserved: [u8; RESERVED_SIZE],
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    #[inline]
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    #[inline]
    fn need(&self, n: usize, field: &str) -> Result<(), String> {
        if self.pos + n <= self.bytes.len() {
            Ok(())
        } else {
            Err(format!(
                "header: not enough bytes for {field} at offset {} (need {n}, have {})",
                self.pos,
                self.bytes.len().saturating_sub(self.pos)
            ))
        }
    }

    #[inline]
    fn read_u8(&mut self, field: &str) -> Result<u8, String> {
        self.need(1, field)?;
        let v = self.bytes[self.pos];
        self.pos += 1;
        Ok(v)
    }

    #[inline]
    fn read_u32_le(&mut self, field: &str) -> Result<u32, String> {
        self.need(4, field)?;
        let v = u32::from_le_bytes(self.bytes[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    #[inline]
    fn read_u64_le(&mut self, field: &str) -> Result<u64, String> {
        self.need(8, field)?;
        let v = u64::from_le_bytes(self.bytes[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    #[inline]
    fn read_arr<const N: usize>(&mut self, field: &str) -> Result<[u8; N], String> {
        self.need(N, field)?;
        let v: [u8; N] = self.bytes[self.pos..self.pos + N].try_into().unwrap();
        self.pos += N;
        Ok(v)
    }
}
