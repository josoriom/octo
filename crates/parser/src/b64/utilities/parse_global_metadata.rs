use crate::{
    b64::utilities::{
        common::decompress_zstd_allow_aligned_padding,
        parse_metadata::{HDR_CODEC_NONE, HDR_CODEC_ZSTD, parse_metadata},
    },
    decode::Metadatum,
};

const GLOBAL_HEADER_SIZE_36: usize = 36;

#[inline]
fn u32_at(bytes: &[u8], off: usize) -> Result<u32, String> {
    let end = off
        .checked_add(4)
        .ok_or_else(|| "global metadata: section too small".to_string())?;
    if end > bytes.len() {
        return Err("global metadata: section too small".to_string());
    }
    Ok(u32::from_le_bytes(bytes[off..end].try_into().unwrap()))
}

pub fn parse_global_metadata(
    bytes: &[u8],
    item_count: u32,
    meta_count: u32,
    num_count: u32,
    str_count: u32,
    compression_codec: u8,
    expected_uncompressed: u64,
) -> Result<Vec<Metadatum>, String> {
    let expected = usize::try_from(expected_uncompressed)
        .map_err(|_| "global metadata: expected_uncompressed overflow".to_string())?;

    let owned;
    let bytes = match compression_codec {
        HDR_CODEC_NONE => {
            if expected == 0 {
                bytes
            } else if bytes.len() < expected {
                return Err("global metadata: section too small".to_string());
            } else if bytes.len() > expected {
                let trailing = &bytes[expected..];
                if trailing.len() > 7 || trailing.iter().any(|&b| b != 0) {
                    return Err("global metadata: trailing bytes".to_string());
                }
                &bytes[..expected]
            } else {
                bytes
            }
        }
        HDR_CODEC_ZSTD => {
            owned = decompress_zstd_allow_aligned_padding(bytes, expected)?;
            owned.as_slice()
        }
        other => return Err(format!("unsupported compression_codec={other}")),
    };

    if bytes.len() < GLOBAL_HEADER_SIZE_36 + 4 {
        return Err("global metadata: section too small".to_string());
    }

    if u32_at(bytes, GLOBAL_HEADER_SIZE_36)? != 0 {
        return Err("global metadata: missing header or corrupted CI".to_string());
    }

    let mut pos = 0usize;

    let n_file_description = u32_at(bytes, pos)?;
    pos += 4;
    let n_run = u32_at(bytes, pos)?;
    pos += 4;
    let n_ref_param_groups = u32_at(bytes, pos)?;
    pos += 4;
    let n_samples = u32_at(bytes, pos)?;
    pos += 4;
    let n_instrument_configs = u32_at(bytes, pos)?;
    pos += 4;
    let n_software = u32_at(bytes, pos)?;
    pos += 4;
    let n_data_processing = u32_at(bytes, pos)?;
    pos += 4;
    let n_acquisition_settings = u32_at(bytes, pos)?;
    pos += 4;
    let n_cvs = u32_at(bytes, pos)?;

    let derived_item_count = n_file_description
        .checked_add(n_run)
        .ok_or_else(|| "global metadata: item_count overflow".to_string())?
        .checked_add(n_ref_param_groups)
        .ok_or_else(|| "global metadata: item_count overflow".to_string())?
        .checked_add(n_samples)
        .ok_or_else(|| "global metadata: item_count overflow".to_string())?
        .checked_add(n_instrument_configs)
        .ok_or_else(|| "global metadata: item_count overflow".to_string())?
        .checked_add(n_software)
        .ok_or_else(|| "global metadata: item_count overflow".to_string())?
        .checked_add(n_data_processing)
        .ok_or_else(|| "global metadata: item_count overflow".to_string())?
        .checked_add(n_acquisition_settings)
        .ok_or_else(|| "global metadata: item_count overflow".to_string())?
        .checked_add(n_cvs)
        .ok_or_else(|| "global metadata: item_count overflow".to_string())?;

    if derived_item_count == 0 {
        return Err("global metadata: header counts are zero".to_string());
    }

    if item_count != 0 && item_count != derived_item_count {
        return Err("global metadata: item_count mismatch".to_string());
    }

    parse_metadata(
        &bytes[GLOBAL_HEADER_SIZE_36..],
        derived_item_count,
        meta_count,
        num_count,
        str_count,
        HDR_CODEC_NONE,
        0,
    )
}
