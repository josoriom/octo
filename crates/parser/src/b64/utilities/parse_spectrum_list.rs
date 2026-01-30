use std::collections::{HashMap, HashSet};

use crate::{
    Spectrum, SpectrumDescription, SpectrumList,
    b64::{
        decode::Metadatum,
        utilities::{
            common::{ChildIndex, get_attr_text, get_attr_u32, xy_lengths_from_bdal},
            parse_binary_data_array_list, parse_cv_and_user_params, parse_precursor_list,
            parse_product_list, parse_scan_list,
        },
    },
    mzml::{schema::TagId, structs::BinaryDataArrayList},
};

use crate::mzml::attr_meta::{
    ACC_ATTR_COUNT, ACC_ATTR_DATA_PROCESSING_REF, ACC_ATTR_DEFAULT_ARRAY_LENGTH,
    ACC_ATTR_DEFAULT_DATA_PROCESSING_REF, ACC_ATTR_ID, ACC_ATTR_INDEX, ACC_ATTR_MS_LEVEL,
    ACC_ATTR_NATIVE_ID, ACC_ATTR_SCAN_NUMBER, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPOT_ID,
};

#[inline]
pub fn parse_spectrum_list(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<SpectrumList> {
    // <spectrumList>/<spectrum>/<cvParam>
    let mut spectrum_list_rows: Vec<&Metadatum> = Vec::new();
    let mut spectrum_rows: Vec<&Metadatum> = Vec::new();

    for &m in metadata {
        match m.tag_id {
            TagId::SpectrumList => spectrum_list_rows.push(m),
            TagId::Spectrum => spectrum_rows.push(m),
            _ => {}
        }
    }

    if spectrum_rows.is_empty() {
        return None;
    }

    let default_data_processing_ref =
        get_attr_text(&spectrum_list_rows, ACC_ATTR_DEFAULT_DATA_PROCESSING_REF)
            .or_else(|| get_attr_text(&spectrum_list_rows, ACC_ATTR_DATA_PROCESSING_REF))
            .or_else(|| get_attr_text(metadata, ACC_ATTR_DEFAULT_DATA_PROCESSING_REF))
            .or_else(|| get_attr_text(metadata, ACC_ATTR_DATA_PROCESSING_REF));

    let count_attr = get_attr_u32(&spectrum_list_rows, ACC_ATTR_COUNT)
        .or_else(|| get_attr_u32(metadata, ACC_ATTR_COUNT))
        .map(|v| v as usize);

    let mut spectrum_owner_to_item: HashMap<u32, u32> = HashMap::with_capacity(spectrum_rows.len());
    for &m in &spectrum_rows {
        spectrum_owner_to_item.insert(m.owner_id, m.item_index);
    }

    let mut spectrum_item_indices: Vec<u32> = Vec::new();

    if !spectrum_list_rows.is_empty() {
        let mut spectrum_list_id: u32 = spectrum_list_rows[0].owner_id;

        for row in &spectrum_list_rows {
            let direct = child_index.ids(row.owner_id, TagId::Spectrum);
            let mut found = false;

            for &spectrum_id in direct {
                if spectrum_owner_to_item.contains_key(&spectrum_id) {
                    spectrum_list_id = row.owner_id;
                    found = true;
                    break;
                }
            }

            if found {
                break;
            }
        }

        let direct = child_index.ids(spectrum_list_id, TagId::Spectrum);
        if !direct.is_empty() {
            let mut out = Vec::with_capacity(direct.len());
            let mut seen = HashSet::with_capacity(direct.len());

            for &spectrum_id in direct {
                if let Some(&item_index) = spectrum_owner_to_item.get(&spectrum_id) {
                    if seen.insert(item_index) {
                        out.push(item_index);
                    }
                }
            }

            if !out.is_empty() {
                spectrum_item_indices = out;
            }
        }
    }

    if spectrum_item_indices.is_empty() {
        let mut seen_item: HashSet<u32> = HashSet::with_capacity(spectrum_rows.len());
        spectrum_item_indices = Vec::with_capacity(spectrum_rows.len());

        for &m in &spectrum_rows {
            if seen_item.insert(m.item_index) {
                spectrum_item_indices.push(m.item_index);
            }
        }
    }

    let n = spectrum_item_indices.len();
    if n == 0 {
        return None;
    }

    let mut pos_by_item: HashMap<u32, usize> = HashMap::with_capacity(n);
    for (pos, item_index) in spectrum_item_indices.iter().copied().enumerate() {
        pos_by_item.insert(item_index, pos);
    }

    let mut counts = vec![0usize; n];
    for &m in metadata {
        if let Some(&pos) = pos_by_item.get(&m.item_index) {
            counts[pos] += 1;
        }
    }

    let mut buckets: Vec<Vec<&Metadatum>> = Vec::with_capacity(n);
    for &c in &counts {
        buckets.push(Vec::with_capacity(c));
    }

    for &m in metadata {
        if let Some(&pos) = pos_by_item.get(&m.item_index) {
            buckets[pos].push(m);
        }
    }

    let mut spectra = Vec::with_capacity(n);

    for (pos, spectrum_meta) in buckets.iter().enumerate() {
        if spectrum_meta.is_empty() {
            continue;
        }

        let mut root_id = 0u32;
        let mut has_other_root = false;
        for &m in spectrum_meta {
            if m.tag_id != TagId::Spectrum {
                continue;
            }
            if root_id == 0 {
                root_id = m.owner_id;
            } else if m.owner_id != root_id {
                has_other_root = true;
                break;
            }
        }

        let scoped_meta: Vec<&Metadatum>;
        let used_meta: &[&Metadatum] = if has_other_root && root_id != 0 {
            scoped_meta = collect_subtree_metadata(spectrum_meta, child_index, root_id);
            scoped_meta.as_slice()
        } else {
            spectrum_meta
        };

        let local_child_index = ChildIndex::new_from_refs(used_meta);

        spectra.push(parse_spectrum(
            used_meta,
            pos as u32,
            default_data_processing_ref.as_deref(),
            &local_child_index,
        ));
    }

    if spectra.is_empty() {
        return None;
    }

    Some(SpectrumList {
        count: count_attr.or(Some(spectra.len())),
        default_data_processing_ref,
        spectra,
    })
}

#[inline]
fn parse_spectrum(
    metadata: &[&Metadatum],
    fallback_index: u32,
    default_data_processing_ref: Option<&str>,
    child_index: &ChildIndex,
) -> Spectrum {
    // <spectrum>
    let mut spectrum_rows: Vec<&Metadatum> = Vec::new();
    let mut spectrum_params_meta: Vec<&Metadatum> = Vec::new();
    let mut spectrum_id: u32 = 0;

    for &m in metadata {
        if m.tag_id != TagId::Spectrum {
            continue;
        }

        if spectrum_id == 0 {
            spectrum_id = m.owner_id;
        }

        spectrum_rows.push(m);

        if m.owner_id != spectrum_id {
            continue;
        }

        if m.accession
            .as_deref()
            .is_some_and(|a| a.starts_with("B000:"))
        {
            continue;
        }

        spectrum_params_meta.push(m);
    }

    let id = get_attr_text(&spectrum_rows, ACC_ATTR_ID).unwrap_or_default();
    let index = get_attr_u32(&spectrum_rows, ACC_ATTR_INDEX).or(Some(fallback_index));

    let scan_number = get_attr_u32(&spectrum_rows, ACC_ATTR_SCAN_NUMBER);
    let ms_level = get_attr_u32(&spectrum_rows, ACC_ATTR_MS_LEVEL);

    let native_id = get_attr_text(&spectrum_rows, ACC_ATTR_NATIVE_ID);
    let source_file_ref = get_attr_text(&spectrum_rows, ACC_ATTR_SOURCE_FILE_REF);
    let spot_id = get_attr_text(&spectrum_rows, ACC_ATTR_SPOT_ID);

    let data_processing_ref = get_attr_text(&spectrum_rows, ACC_ATTR_DATA_PROCESSING_REF)
        .or_else(|| default_data_processing_ref.map(|s| s.to_string()));

    let (cv_params, user_params) = parse_cv_and_user_params(&spectrum_params_meta);

    let scan_list = parse_scan_list(metadata, child_index);
    let product_list = parse_product_list(metadata, child_index);
    let precursor_list = parse_precursor_list(metadata, child_index);

    let binary_data_array_list: Option<BinaryDataArrayList> =
        parse_binary_data_array_list(metadata);

    let (x_len, y_len) = xy_lengths_from_bdal(binary_data_array_list.as_ref());

    let default_array_length_attr =
        get_attr_u32(&spectrum_rows, ACC_ATTR_DEFAULT_ARRAY_LENGTH).map(|v| v as usize);

    let default_array_length = default_array_length_attr.or(x_len).or(y_len).or(Some(0));
    let spectrum_description = parse_spectrum_description(metadata, child_index);

    Spectrum {
        id,
        index,
        scan_number,
        default_array_length,
        native_id,
        data_processing_ref,
        source_file_ref,
        spot_id,
        ms_level,
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
        spectrum_description,
        scan_list,
        precursor_list,
        product_list,
        binary_data_array_list,
    }
}

#[inline]
fn parse_spectrum_description(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<SpectrumDescription> {
    // <spectrumDescription>
    let mut spectrum_id: u32 = 0;
    for &m in metadata {
        if m.tag_id == TagId::Spectrum {
            spectrum_id = m.owner_id;
            break;
        }
    }
    if spectrum_id == 0 {
        return None;
    }

    let sd_id = child_index
        .first_id(spectrum_id, TagId::SpectrumDescription)
        .or_else(|| {
            metadata
                .iter()
                .find(|m| m.tag_id == TagId::SpectrumDescription && m.parent_index == spectrum_id)
                .map(|m| m.owner_id)
        })?;

    let sd_meta = collect_subtree_metadata(metadata, child_index, sd_id);
    let sd_child_index = ChildIndex::new_from_refs(sd_meta.as_slice());

    let mut sd_rows: Vec<&Metadatum> = Vec::new();
    for &m in &sd_meta {
        if m.tag_id != TagId::SpectrumDescription || m.owner_id != sd_id {
            continue;
        }
        if m.accession
            .as_deref()
            .is_some_and(|a| a.starts_with("B000:"))
        {
            continue;
        }
        sd_rows.push(m);
    }

    let (cv_params, user_params) = parse_cv_and_user_params(&sd_rows);

    let scan_list = parse_scan_list(sd_meta.as_slice(), &sd_child_index);
    let product_list = parse_product_list(sd_meta.as_slice(), &sd_child_index);
    let precursor_list = parse_precursor_list(sd_meta.as_slice(), &sd_child_index);

    Some(SpectrumDescription {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
        scan_list,
        precursor_list,
        product_list,
    })
}

#[inline]
fn collect_subtree_metadata<'a>(
    metadata: &'a [&'a Metadatum],
    child_index: &ChildIndex,
    root_owner_id: u32,
) -> Vec<&'a Metadatum> {
    let mut keep: HashSet<u32> = HashSet::with_capacity(64);
    let mut stack: Vec<u32> = Vec::with_capacity(32);
    stack.push(root_owner_id);

    while let Some(id) = stack.pop() {
        if !keep.insert(id) {
            continue;
        }
        for &ch in child_index.children(id) {
            stack.push(ch);
        }
    }

    let mut out: Vec<&'a Metadatum> = Vec::with_capacity(keep.len().min(metadata.len()));
    for &m in metadata {
        if keep.contains(&m.owner_id) {
            out.push(m);
        }
    }

    out
}
