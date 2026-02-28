use crate::b64::utilities::parse_header::{
    HEADER_ARRAY_FILTER_ID, HEADER_CHROM_ARRAY_TYPE_COUNT, HEADER_CHROM_BLOCK_COUNT,
    HEADER_CHROM_COUNT, HEADER_CHROM_META_NUMERIC_COUNT, HEADER_CHROM_META_ROW_COUNT,
    HEADER_CHROM_META_STRING_COUNT, HEADER_CHROM_META_UNCOMPRESSED_SIZE, HEADER_CODEC_ID,
    HEADER_COMPRESSION_LEVEL, HEADER_GLOBAL_META_NUMERIC_COUNT, HEADER_GLOBAL_META_ROW_COUNT,
    HEADER_GLOBAL_META_STRING_COUNT, HEADER_GLOBAL_META_UNCOMPRESSED_SIZE,
    HEADER_LEN_CHROM_ARRAYREFS, HEADER_LEN_CHROM_ENTRIES, HEADER_LEN_CHROM_META,
    HEADER_LEN_GLOBAL_META, HEADER_LEN_PACKED_CHROMS, HEADER_LEN_PACKED_SPECTRA,
    HEADER_LEN_SPEC_ARRAYREFS, HEADER_LEN_SPEC_ENTRIES, HEADER_LEN_SPEC_META,
    HEADER_OFFSET_CHROM_ARRAYREFS, HEADER_OFFSET_CHROM_ENTRIES, HEADER_OFFSET_CHROM_META,
    HEADER_OFFSET_GLOBAL_META, HEADER_OFFSET_PACKED_CHROMS, HEADER_OFFSET_PACKED_SPECTRA,
    HEADER_OFFSET_SPEC_ARRAYREFS, HEADER_OFFSET_SPEC_ENTRIES, HEADER_OFFSET_SPEC_META,
    HEADER_SPEC_ARRAY_TYPE_COUNT, HEADER_SPEC_META_NUMERIC_COUNT, HEADER_SPEC_META_ROW_COUNT,
    HEADER_SPEC_META_STRING_COUNT, HEADER_SPEC_META_UNCOMPRESSED_SIZE, HEADER_SPECTRUM_BLOCK_COUNT,
    HEADER_SPECTRUM_COUNT, HEADER_TARGET_BLOCK_SIZE,
};

#[derive(Default)]
pub(crate) struct FileHeader {
    pub(crate) offset_spec_entries: u64,
    pub(crate) len_spec_entries: u64,
    pub(crate) offset_spec_arrayrefs: u64,
    pub(crate) len_spec_arrayrefs: u64,
    pub(crate) offset_chrom_entries: u64,
    pub(crate) len_chrom_entries: u64,
    pub(crate) offset_chrom_arrayrefs: u64,
    pub(crate) len_chrom_arrayrefs: u64,
    pub(crate) offset_spec_meta: u64,
    pub(crate) len_spec_meta: u64,
    pub(crate) offset_chrom_meta: u64,
    pub(crate) len_chrom_meta: u64,
    pub(crate) offset_global_meta: u64,
    pub(crate) len_global_meta: u64,
    pub(crate) offset_packed_spectra: u64,
    pub(crate) len_packed_spectra: u64,
    pub(crate) offset_packed_chroms: u64,
    pub(crate) len_packed_chroms: u64,
    pub(crate) spectrum_block_count: u32,
    pub(crate) chrom_block_count: u32,
    pub(crate) spectrum_count: u32,
    pub(crate) chrom_count: u32,
    pub(crate) spec_meta_row_count: u32,
    pub(crate) spec_meta_numeric_count: u32,
    pub(crate) spec_meta_string_count: u32,
    pub(crate) chrom_meta_row_count: u32,
    pub(crate) chrom_meta_numeric_count: u32,
    pub(crate) chrom_meta_string_count: u32,
    pub(crate) global_meta_row_count: u32,
    pub(crate) global_meta_numeric_count: u32,
    pub(crate) global_meta_string_count: u32,
    pub(crate) spec_array_type_count: u32,
    pub(crate) chrom_array_type_count: u32,
    pub(crate) target_block_size: u64,
    pub(crate) codec_id: u8,
    pub(crate) compression_level: u8,
    pub(crate) array_filter_id: u8,
    pub(crate) spec_meta_uncompressed_size: u64,
    pub(crate) chrom_meta_uncompressed_size: u64,
    pub(crate) global_meta_uncompressed_size: u64,
}

impl FileHeader {
    pub(crate) fn write_into(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(b"B000");
        patch_u64_at(buf, HEADER_OFFSET_SPEC_ENTRIES, self.offset_spec_entries);
        patch_u64_at(buf, HEADER_LEN_SPEC_ENTRIES, self.len_spec_entries);
        patch_u64_at(
            buf,
            HEADER_OFFSET_SPEC_ARRAYREFS,
            self.offset_spec_arrayrefs,
        );
        patch_u64_at(buf, HEADER_LEN_SPEC_ARRAYREFS, self.len_spec_arrayrefs);
        patch_u64_at(buf, HEADER_OFFSET_CHROM_ENTRIES, self.offset_chrom_entries);
        patch_u64_at(buf, HEADER_LEN_CHROM_ENTRIES, self.len_chrom_entries);
        patch_u64_at(
            buf,
            HEADER_OFFSET_CHROM_ARRAYREFS,
            self.offset_chrom_arrayrefs,
        );
        patch_u64_at(buf, HEADER_LEN_CHROM_ARRAYREFS, self.len_chrom_arrayrefs);
        patch_u64_at(buf, HEADER_OFFSET_SPEC_META, self.offset_spec_meta);
        patch_u64_at(buf, HEADER_LEN_SPEC_META, self.len_spec_meta);
        patch_u64_at(buf, HEADER_OFFSET_CHROM_META, self.offset_chrom_meta);
        patch_u64_at(buf, HEADER_LEN_CHROM_META, self.len_chrom_meta);
        patch_u64_at(buf, HEADER_OFFSET_GLOBAL_META, self.offset_global_meta);
        patch_u64_at(buf, HEADER_LEN_GLOBAL_META, self.len_global_meta);
        patch_u64_at(
            buf,
            HEADER_OFFSET_PACKED_SPECTRA,
            self.offset_packed_spectra,
        );
        patch_u64_at(buf, HEADER_LEN_PACKED_SPECTRA, self.len_packed_spectra);
        patch_u64_at(buf, HEADER_OFFSET_PACKED_CHROMS, self.offset_packed_chroms);
        patch_u64_at(buf, HEADER_LEN_PACKED_CHROMS, self.len_packed_chroms);
        patch_u32_at(buf, HEADER_SPECTRUM_BLOCK_COUNT, self.spectrum_block_count);
        patch_u32_at(buf, HEADER_CHROM_BLOCK_COUNT, self.chrom_block_count);
        patch_u32_at(buf, HEADER_SPECTRUM_COUNT, self.spectrum_count);
        patch_u32_at(buf, HEADER_CHROM_COUNT, self.chrom_count);
        patch_u32_at(buf, HEADER_SPEC_META_ROW_COUNT, self.spec_meta_row_count);
        patch_u32_at(
            buf,
            HEADER_SPEC_META_NUMERIC_COUNT,
            self.spec_meta_numeric_count,
        );
        patch_u32_at(
            buf,
            HEADER_SPEC_META_STRING_COUNT,
            self.spec_meta_string_count,
        );
        patch_u32_at(buf, HEADER_CHROM_META_ROW_COUNT, self.chrom_meta_row_count);
        patch_u32_at(
            buf,
            HEADER_CHROM_META_NUMERIC_COUNT,
            self.chrom_meta_numeric_count,
        );
        patch_u32_at(
            buf,
            HEADER_CHROM_META_STRING_COUNT,
            self.chrom_meta_string_count,
        );
        patch_u32_at(
            buf,
            HEADER_GLOBAL_META_ROW_COUNT,
            self.global_meta_row_count,
        );
        patch_u32_at(
            buf,
            HEADER_GLOBAL_META_NUMERIC_COUNT,
            self.global_meta_numeric_count,
        );
        patch_u32_at(
            buf,
            HEADER_GLOBAL_META_STRING_COUNT,
            self.global_meta_string_count,
        );
        patch_u32_at(
            buf,
            HEADER_SPEC_ARRAY_TYPE_COUNT,
            self.spec_array_type_count,
        );
        patch_u32_at(
            buf,
            HEADER_CHROM_ARRAY_TYPE_COUNT,
            self.chrom_array_type_count,
        );
        patch_u64_at(buf, HEADER_TARGET_BLOCK_SIZE, self.target_block_size);
        patch_u8_at(buf, HEADER_CODEC_ID, self.codec_id);
        patch_u8_at(buf, HEADER_COMPRESSION_LEVEL, self.compression_level);
        patch_u8_at(buf, HEADER_ARRAY_FILTER_ID, self.array_filter_id);
        patch_u64_at(
            buf,
            HEADER_SPEC_META_UNCOMPRESSED_SIZE,
            self.spec_meta_uncompressed_size,
        );
        patch_u64_at(
            buf,
            HEADER_CHROM_META_UNCOMPRESSED_SIZE,
            self.chrom_meta_uncompressed_size,
        );
        patch_u64_at(
            buf,
            HEADER_GLOBAL_META_UNCOMPRESSED_SIZE,
            self.global_meta_uncompressed_size,
        );
    }
}

fn patch_u8_at(buf: &mut [u8], offset: usize, value: u8) {
    buf[offset] = value;
}
fn patch_u32_at(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}
fn patch_u64_at(buf: &mut [u8], offset: usize, value: u64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
