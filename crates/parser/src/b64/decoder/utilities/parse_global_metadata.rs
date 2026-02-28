use crate::{
    b64::utilities::{
        common::{decompress_zstd_allow_aligned_padding, read_u32_le_at},
        parse_metadata::{HDR_CODEC_NONE, HDR_CODEC_ZSTD, parse_metadata},
    },
    decoder::decode::Metadatum,
};

const GLOBAL_SECTION_HEADER_BYTE_SIZE: usize = 36;

pub(crate) fn parse_global_metadata(
    bytes: &[u8],
    item_count: u32,
    meta_count: u32,
    num_count: u32,
    str_count: u32,
    compression_codec: u8,
    expected_uncompressed: u64,
) -> Result<Vec<Metadatum>, String> {
    let expected_byte_count = usize::try_from(expected_uncompressed)
        .map_err(|_| "global metadata: expected_uncompressed overflow".to_string())?;

    let owned;
    let bytes = match compression_codec {
        HDR_CODEC_NONE => {
            if expected_byte_count == 0 {
                bytes
            } else if bytes.len() < expected_byte_count {
                return Err("global metadata: section too small".to_string());
            } else if bytes.len() > expected_byte_count {
                let trailing = &bytes[expected_byte_count..];
                if trailing.len() > 7 || trailing.iter().any(|&b| b != 0) {
                    return Err("global metadata: trailing bytes".to_string());
                }
                &bytes[..expected_byte_count]
            } else {
                bytes
            }
        }
        HDR_CODEC_ZSTD => {
            owned = decompress_zstd_allow_aligned_padding(bytes, expected_byte_count)?;
            owned.as_slice()
        }
        other => return Err(format!("unsupported compression_codec={other}")),
    };

    if bytes.len() < GLOBAL_SECTION_HEADER_BYTE_SIZE + 4 {
        return Err("global metadata: section too small".to_string());
    }

    let mut read_pos = 0usize;

    let n_file_description = read_u32_le_at(bytes, &mut read_pos, "n_file_description")?;
    let n_run = read_u32_le_at(bytes, &mut read_pos, "n_run")?;
    let n_ref_param_groups = read_u32_le_at(bytes, &mut read_pos, "n_ref_param_groups")?;
    let n_samples = read_u32_le_at(bytes, &mut read_pos, "n_samples")?;
    let n_instrument_configs = read_u32_le_at(bytes, &mut read_pos, "n_instrument_configs")?;
    let n_software = read_u32_le_at(bytes, &mut read_pos, "n_software")?;
    let n_data_processing = read_u32_le_at(bytes, &mut read_pos, "n_data_processing")?;
    let n_acquisition_settings = read_u32_le_at(bytes, &mut read_pos, "n_acquisition_settings")?;
    let n_cvs = read_u32_le_at(bytes, &mut read_pos, "n_cvs")?;

    let derived_item_count = [
        n_file_description,
        n_run,
        n_ref_param_groups,
        n_samples,
        n_instrument_configs,
        n_software,
        n_data_processing,
        n_acquisition_settings,
        n_cvs,
    ]
    .iter()
    .try_fold(0u32, |acc, &count| {
        acc.checked_add(count)
            .ok_or("global metadata: item_count overflow")
    })?;

    if derived_item_count == 0 {
        return Err("global metadata: header counts are zero".to_string());
    }

    if item_count != 0 && item_count != derived_item_count {
        return Err("global metadata: item_count mismatch".to_string());
    }

    let first_index_entry = read_u32_le_at(bytes, &mut read_pos, "first_index_entry")?;
    if first_index_entry != 0 {
        return Err("global metadata: missing header or corrupted CI".to_string());
    }

    parse_metadata(
        &bytes[GLOBAL_SECTION_HEADER_BYTE_SIZE..],
        derived_item_count,
        meta_count,
        num_count,
        str_count,
        HDR_CODEC_NONE,
        0,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_u32_le(buf: &mut Vec<u8>, value: u32) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    fn make_global_header(counts: &[u32; 9]) -> Vec<u8> {
        let mut buf = Vec::new();
        for &count in counts {
            write_u32_le(&mut buf, count);
        }
        buf
    }

    #[test]
    fn parse_global_metadata_rejects_section_that_is_too_small() {
        let tiny = vec![0u8; 4];
        let result = parse_global_metadata(&tiny, 0, 0, 0, 0, HDR_CODEC_NONE, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too small"));
    }

    #[test]
    fn parse_global_metadata_rejects_zero_counts_in_header() {
        let mut bytes = make_global_header(&[0, 0, 0, 0, 0, 0, 0, 0, 0]);
        write_u32_le(&mut bytes, 0);
        bytes.resize(GLOBAL_SECTION_HEADER_BYTE_SIZE + 4, 0);

        let result = parse_global_metadata(&bytes, 0, 0, 0, 0, HDR_CODEC_NONE, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("counts are zero"));
    }

    #[test]
    fn parse_global_metadata_rejects_item_count_mismatch() {
        let mut bytes = make_global_header(&[1, 1, 0, 0, 0, 0, 0, 0, 0]);
        write_u32_le(&mut bytes, 0);
        bytes.resize(GLOBAL_SECTION_HEADER_BYTE_SIZE + 4, 0);

        let result = parse_global_metadata(&bytes, 99, 0, 0, 0, HDR_CODEC_NONE, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mismatch"));
    }

    #[test]
    fn parse_global_metadata_rejects_unsupported_codec() {
        let bytes = vec![0u8; GLOBAL_SECTION_HEADER_BYTE_SIZE + 4];
        let result = parse_global_metadata(&bytes, 0, 0, 0, 0, 99, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unsupported"));
    }

    #[test]
    fn parse_global_metadata_rejects_trailing_nonzero_bytes_for_uncompressed() {
        let expected = 8usize;
        let mut bytes = vec![0u8; expected];
        bytes.extend_from_slice(&[0u8, 0, 1]);

        let result = parse_global_metadata(&bytes, 0, 0, 0, 0, HDR_CODEC_NONE, expected as u64);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("trailing bytes"));
    }

    #[test]
    fn parse_global_metadata_accepts_zero_padding_within_seven_bytes_for_uncompressed() {
        let expected = 8usize;
        let mut bytes = vec![1u8; expected];
        bytes.extend_from_slice(&[0u8; 4]);

        let result = parse_global_metadata(&bytes, 0, 0, 0, 0, HDR_CODEC_NONE, expected as u64);
        assert!(result.is_err());
        assert!(!result.unwrap_err().contains("trailing bytes"));
    }
}
