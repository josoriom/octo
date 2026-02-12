use std::collections::{HashMap, HashSet};

use crate::{
    Spectrum, SpectrumDescription, SpectrumList,
    b64::{
        decode::Metadatum,
        utilities::{
            children_lookup::ChildrenLookup,
            common::{get_attr_text, get_attr_u32, xy_lengths_from_bdal},
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
    children_lookup: &ChildrenLookup,
) -> Option<SpectrumList> {
    let mut spectrum_list_rows: Vec<&Metadatum> = Vec::new();
    for &m in metadata {
        if m.tag_id == TagId::SpectrumList {
            spectrum_list_rows.push(m);
        }
    }

    let spectrum_ids = ChildrenLookup::all_ids(metadata, TagId::Spectrum);
    if spectrum_ids.is_empty() {
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

    let mut spectrum_owner_to_item: HashMap<u32, u32> = HashMap::with_capacity(spectrum_ids.len());
    for &m in metadata {
        if m.tag_id == TagId::Spectrum {
            spectrum_owner_to_item.entry(m.id).or_insert(m.item_index);
        }
    }

    let mut items: Vec<(u32, u32, u32)> = Vec::with_capacity(spectrum_ids.len());
    let mut item_indices: Vec<u32> = Vec::new();
    let mut seen_items: HashSet<u32> = HashSet::with_capacity(spectrum_ids.len());

    for (fallback_index, &spectrum_id) in spectrum_ids.iter().enumerate() {
        let Some(&item_idx) = spectrum_owner_to_item.get(&spectrum_id) else {
            continue;
        };
        items.push((spectrum_id, item_idx, fallback_index as u32));
        if seen_items.insert(item_idx) {
            item_indices.push(item_idx);
        }
    }

    if items.is_empty() {
        return None;
    }

    let mut pos_by_item: HashMap<u32, usize> = HashMap::with_capacity(item_indices.len());
    for (pos, item_idx) in item_indices.iter().copied().enumerate() {
        pos_by_item.insert(item_idx, pos);
    }

    let mut counts = vec![0usize; item_indices.len()];
    for &m in metadata {
        if let Some(&pos) = pos_by_item.get(&m.item_index) {
            counts[pos] += 1;
        }
    }

    let mut buckets: Vec<Vec<&Metadatum>> = Vec::with_capacity(item_indices.len());
    for &c in &counts {
        buckets.push(Vec::with_capacity(c));
    }

    for &m in metadata {
        if let Some(&pos) = pos_by_item.get(&m.item_index) {
            buckets[pos].push(m);
        }
    }

    let mut has_other_root_by_bucket = vec![false; buckets.len()];
    for (pos, bucket) in buckets.iter().enumerate() {
        let mut root_id = 0u32;
        for &m in bucket {
            if m.tag_id != TagId::Spectrum {
                continue;
            }
            if root_id == 0 {
                root_id = m.id;
            } else if m.id != root_id {
                has_other_root_by_bucket[pos] = true;
                break;
            }
        }
    }

    let mut spectra = Vec::with_capacity(items.len());

    for (spectrum_id, item_idx, fallback_index) in items {
        let Some(&pos) = pos_by_item.get(&item_idx) else {
            continue;
        };
        let item_meta = buckets[pos].as_slice();
        if item_meta.is_empty() {
            continue;
        }

        if has_other_root_by_bucket[pos] {
            let keep = children_lookup.subtree_ids(spectrum_id);
            let mut scoped: Vec<&Metadatum> = Vec::with_capacity(item_meta.len());
            for &m in item_meta {
                if keep.contains(&m.id) {
                    scoped.push(m);
                }
            }
            if scoped.is_empty() {
                continue;
            }
            let local_children_lookup = ChildrenLookup::new(scoped.as_slice());
            spectra.push(parse_spectrum(
                scoped.as_slice(),
                fallback_index,
                default_data_processing_ref.as_deref(),
                &local_children_lookup,
            ));
        } else {
            let local_children_lookup = ChildrenLookup::new(item_meta);
            spectra.push(parse_spectrum(
                item_meta,
                fallback_index,
                default_data_processing_ref.as_deref(),
                &local_children_lookup,
            ));
        }
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
    children_lookup: &ChildrenLookup,
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
            spectrum_id = m.id;
        }

        spectrum_rows.push(m);

        if m.id != spectrum_id {
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

    let scan_list = parse_scan_list(metadata, children_lookup);
    let product_list = parse_product_list(metadata, children_lookup);
    let precursor_list = parse_precursor_list(metadata, children_lookup);

    let binary_data_array_list: Option<BinaryDataArrayList> =
        parse_binary_data_array_list(metadata);

    let (x_len, y_len) = xy_lengths_from_bdal(binary_data_array_list.as_ref());

    let default_array_length_attr =
        get_attr_u32(&spectrum_rows, ACC_ATTR_DEFAULT_ARRAY_LENGTH).map(|v| v as usize);

    let default_array_length = default_array_length_attr.or(x_len).or(y_len).or(Some(0));
    let spectrum_description = parse_spectrum_description(metadata, children_lookup);

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
    children_lookup: &ChildrenLookup,
) -> Option<SpectrumDescription> {
    // <spectrumDescription>
    let mut spectrum_id: u32 = 0;
    for &m in metadata {
        if m.tag_id == TagId::Spectrum {
            spectrum_id = m.id;
            break;
        }
    }
    if spectrum_id == 0 {
        return None;
    }

    let sd_id = children_lookup
        .first_id(spectrum_id, TagId::SpectrumDescription)
        .or_else(|| {
            metadata
                .iter()
                .find(|m| m.tag_id == TagId::SpectrumDescription && m.parent_index == spectrum_id)
                .map(|m| m.id)
        })?;

    let keep = children_lookup.subtree_ids(sd_id);
    let mut sd_meta: Vec<&Metadatum> = Vec::with_capacity(keep.len().min(metadata.len()));
    for &m in metadata {
        if keep.contains(&m.id) {
            sd_meta.push(m);
        }
    }
    let sd_children_lookup = ChildrenLookup::new(sd_meta.as_slice());

    let mut sd_rows: Vec<&Metadatum> = Vec::new();
    for &m in &sd_meta {
        if m.tag_id != TagId::SpectrumDescription || m.id != sd_id {
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

    let scan_list = parse_scan_list(sd_meta.as_slice(), &sd_children_lookup);
    let product_list = parse_product_list(sd_meta.as_slice(), &sd_children_lookup);
    let precursor_list = parse_precursor_list(sd_meta.as_slice(), &sd_children_lookup);

    Some(SpectrumDescription {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
        scan_list,
        precursor_list,
        product_list,
    })
}
