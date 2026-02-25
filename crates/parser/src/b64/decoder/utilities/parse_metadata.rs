use crate::{
    b64::{attr_meta::format_accession, utilities::common::*},
    decoder::{
        decode::{Metadatum, MetadatumValue},
        utilities::common::{
            decompress_zstd_allow_aligned_padding, read_u32_vec, take, vs_len_bytes,
        },
    },
    mzml::schema::TagId,
};

pub(crate) const HDR_CODEC_NONE: u8 = 0;
pub(crate) const HDR_CODEC_ZSTD: u8 = 1;

pub(crate) fn parse_metadata(
    bytes: &[u8],
    item_count: u32,
    meta_count: u32,
    num_count: u32,
    str_count: u32,
    compression_codec: u8,
    expected_uncompressed_bytes: usize,
) -> Result<Vec<Metadatum>, String> {
    let owned;
    let bytes = match compression_codec {
        HDR_CODEC_NONE => bytes,
        HDR_CODEC_ZSTD => {
            owned = decompress_zstd_allow_aligned_padding(bytes, expected_uncompressed_bytes)?;
            owned.as_slice()
        }
        other => return Err(format!("unsupported compression_codec={other}")),
    };

    let item_count = item_count as usize;
    let meta_count = meta_count as usize;
    let num_count = num_count as usize;
    let str_count = str_count as usize;

    let mut pos = 0usize;

    let children_index = read_u32_vec(bytes, &mut pos, item_count + 1)?;
    let metadatum_owner_ids = read_u32_vec(bytes, &mut pos, meta_count)?;
    let metadatum_parent_ids = read_u32_vec(bytes, &mut pos, meta_count)?;
    let metadatum_tag_ids = take(bytes, &mut pos, meta_count, "metadatum tag id")?;
    let metadatum_ref_ids = take(bytes, &mut pos, meta_count, "metadatum ref id")?;
    let metadatum_accessions = read_u32_vec(bytes, &mut pos, meta_count)?;
    let metadatum_unit_refs = take(bytes, &mut pos, meta_count, "metadatum unit ref id")?;
    let metadatum_unit_accs = read_u32_vec(bytes, &mut pos, meta_count)?;
    let value_kinds = take(bytes, &mut pos, meta_count, "metadatum value kind")?;
    let value_indices = read_u32_vec(bytes, &mut pos, meta_count)?;

    let numeric_values = read_f64_vec(bytes, &mut pos, num_count)?;
    let string_offsets = read_u32_vec(bytes, &mut pos, str_count)?;
    let string_lengths = read_u32_vec(bytes, &mut pos, str_count)?;

    let string_bytes_needed = vs_len_bytes(
        value_kinds,
        &value_indices,
        &string_offsets,
        &string_lengths,
    )?;
    let string_data = take(bytes, &mut pos, string_bytes_needed, "string values")?;

    validate_trailing_bytes(bytes, pos, compression_codec, expected_uncompressed_bytes)?;
    validate_children_index(&children_index, item_count, meta_count)?;

    let mut out = Vec::with_capacity(meta_count);

    for item_index in 0..item_count {
        let meta_start = children_index[item_index] as usize;
        let meta_end = children_index[item_index + 1] as usize;

        for meta_index in meta_start..meta_end {
            let tag_id = TagId::from_u8(metadatum_tag_ids[meta_index]).unwrap_or(TagId::Unknown);
            let value = parse_value(
                value_kinds[meta_index],
                value_indices[meta_index],
                &numeric_values,
                &string_offsets,
                &string_lengths,
                string_data,
            )?;

            let accession = format_accession(
                metadatum_ref_ids[meta_index],
                metadatum_accessions[meta_index],
            );
            let unit_accession = format_accession(
                metadatum_unit_refs[meta_index],
                metadatum_unit_accs[meta_index],
            );

            out.push(Metadatum {
                item_index: item_index as u32,
                id: metadatum_owner_ids[meta_index],
                parent_id: metadatum_parent_ids[meta_index],
                tag_id,
                accession,
                unit_accession,
                value,
            });
        }
    }

    Ok(out)
}

#[inline]
fn validate_trailing_bytes(
    bytes: &[u8],
    pos: usize,
    compression_codec: u8,
    expected_uncompressed_bytes: usize,
) -> Result<(), String> {
    if compression_codec == HDR_CODEC_ZSTD {
        if pos != bytes.len() {
            return Err("trailing bytes in decompressed metadata section".to_string());
        }
    } else {
        let trailing = &bytes[pos..];
        if trailing.len() > 7 || trailing.iter().any(|&b| b != 0) {
            return Err("trailing bytes in metadata section".to_string());
        }
        let _ = expected_uncompressed_bytes;
    }
    Ok(())
}

#[inline]
fn validate_children_index(
    children_index: &[u32],
    item_count: usize,
    meta_count: usize,
) -> Result<(), String> {
    if children_index.is_empty() || children_index[0] != 0 {
        return Err("CI[0] must be 0".to_string());
    }
    if children_index[item_count] as usize != meta_count {
        return Err("CI[last] must equal meta_count".to_string());
    }
    let mut previous = 0u32;
    for &entry in children_index {
        if entry < previous || (entry as usize) > meta_count {
            return Err("CI is not monotonic or out of range".to_string());
        }
        previous = entry;
    }
    Ok(())
}

#[inline]
fn parse_value(
    value_kind: u8,
    value_index: u32,
    numeric_values: &[f64],
    string_offsets: &[u32],
    string_lengths: &[u32],
    string_data: &[u8],
) -> Result<MetadatumValue, String> {
    match value_kind {
        0 => {
            let index = value_index as usize;
            if index >= numeric_values.len() {
                return Err("numeric VI out of range".to_string());
            }
            Ok(MetadatumValue::Number(numeric_values[index]))
        }
        1 => {
            let index = value_index as usize;
            if index >= string_offsets.len() || index >= string_lengths.len() {
                return Err("string VI out of range".to_string());
            }
            let offset = string_offsets[index] as usize;
            let length = string_lengths[index] as usize;
            if offset
                .checked_add(length)
                .map_or(true, |end| end > string_data.len())
            {
                return Err("string slice out of bounds".to_string());
            }
            if length == 0 {
                return Ok(MetadatumValue::Text(String::new()));
            }
            let slice = &string_data[offset..offset + length];
            let text = match std::str::from_utf8(slice) {
                Ok(s) => s.to_string(),
                Err(_) => String::from_utf8_lossy(slice).into_owned(),
            };
            Ok(MetadatumValue::Text(text))
        }
        2 => Ok(MetadatumValue::Empty),
        other => Err(format!("invalid value kind VK={other}")),
    }
}
