use std::collections::{HashMap, HashSet};

use crate::{
    Chromatogram, ChromatogramList,
    b64::utilities::{
        children_lookup::ChildrenLookup,
        common::{get_attr_text, get_attr_u32, xy_lengths_from_bdal},
        parse_binary_data_array_list, parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        schema::TagId,
        structs::{Activation, IsolationWindow, Precursor, Product, SelectedIon, SelectedIonList},
    },
};

use crate::mzml::attr_meta::{
    ACC_ATTR_COUNT, ACC_ATTR_DATA_PROCESSING_REF, ACC_ATTR_DEFAULT_ARRAY_LENGTH,
    ACC_ATTR_DEFAULT_DATA_PROCESSING_REF, ACC_ATTR_EXTERNAL_SPECTRUM_ID, ACC_ATTR_ID,
    ACC_ATTR_INDEX, ACC_ATTR_NATIVE_ID, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPECTRUM_REF,
};

/// <chromatogramList>
#[inline]
pub fn parse_chromatogram_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<ChromatogramList> {
    let mut chromatogram_list_rows: Vec<&Metadatum> = Vec::new();
    for &m in metadata {
        if m.tag_id == TagId::ChromatogramList {
            chromatogram_list_rows.push(m);
        }
    }

    if chromatogram_list_rows.is_empty() {
        return None;
    }

    let default_data_processing_ref = get_attr_text(
        &chromatogram_list_rows,
        ACC_ATTR_DEFAULT_DATA_PROCESSING_REF,
    );

    let count_attr = get_attr_u32(&chromatogram_list_rows, ACC_ATTR_COUNT).map(|v| v as usize);

    let chromatogram_ids = ChildrenLookup::all_ids(metadata, TagId::Chromatogram);
    if chromatogram_ids.is_empty() {
        return None;
    }

    let mut chromatogram_owner_to_item: HashMap<u32, u32> =
        HashMap::with_capacity(chromatogram_ids.len());
    for &m in metadata {
        if m.tag_id == TagId::Chromatogram {
            chromatogram_owner_to_item
                .entry(m.id)
                .or_insert(m.item_index);
        }
    }

    let mut items: Vec<(u32, u32, u32)> = Vec::with_capacity(chromatogram_ids.len());
    let mut item_indices: Vec<u32> = Vec::new();
    let mut seen_items: HashSet<u32> = HashSet::with_capacity(chromatogram_ids.len());

    for (fallback_index, &chromatogram_id) in chromatogram_ids.iter().enumerate() {
        let Some(&item_idx) = chromatogram_owner_to_item.get(&chromatogram_id) else {
            continue;
        };
        items.push((chromatogram_id, item_idx, fallback_index as u32));
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
            if m.tag_id != TagId::Chromatogram {
                continue;
            }
            if root_id == 0 {
                root_id = m.id;
                continue;
            }
            if root_id != m.id {
                has_other_root_by_bucket[pos] = true;
                break;
            }
        }
    }

    let mut chromatograms = Vec::with_capacity(chromatogram_ids.len());

    for (chromatogram_id, item_idx, fallback_index) in items {
        let Some(&pos) = pos_by_item.get(&item_idx) else {
            continue;
        };
        let item_meta = buckets[pos].as_slice();
        if item_meta.is_empty() {
            continue;
        }

        if has_other_root_by_bucket[pos] {
            let keep = children_lookup.subtree_ids(chromatogram_id);
            let mut scoped: Vec<&Metadatum> = Vec::with_capacity(item_meta.len());
            for &m in item_meta {
                if keep.contains(&m.id) {
                    scoped.push(m);
                }
            }

            chromatograms.push(parse_chromatogram(
                scoped.as_slice(),
                chromatogram_id,
                children_lookup,
                fallback_index,
                default_data_processing_ref.as_deref(),
            ));
        } else {
            chromatograms.push(parse_chromatogram(
                item_meta,
                chromatogram_id,
                children_lookup,
                fallback_index,
                default_data_processing_ref.as_deref(),
            ));
        }
    }

    Some(ChromatogramList {
        count: count_attr.or(Some(chromatograms.len())),
        default_data_processing_ref,
        chromatograms,
    })
}

/// <chromatogram>
#[inline]
fn parse_chromatogram(
    metadata: &[&Metadatum],
    chromatogram_id: u32,
    children_lookup: &ChildrenLookup,
    fallback_index: u32,
    default_data_processing_ref: Option<&str>,
) -> Chromatogram {
    let mut chromatogram_rows: Vec<&Metadatum> = Vec::new();
    let mut chromatogram_params_meta: Vec<&Metadatum> = Vec::new();

    for &m in metadata {
        if m.id == chromatogram_id && m.tag_id == TagId::Chromatogram {
            chromatogram_rows.push(m);
            chromatogram_params_meta.push(m);
            continue;
        }

        if m.parent_index == chromatogram_id
            && (m.tag_id == TagId::CvParam || m.tag_id == TagId::UserParam)
        {
            chromatogram_params_meta.push(m);
        }
    }

    let id = get_attr_text(&chromatogram_rows, ACC_ATTR_ID).unwrap_or_default();
    let index = get_attr_u32(&chromatogram_rows, ACC_ATTR_INDEX).or(Some(fallback_index));
    let native_id = get_attr_text(&chromatogram_rows, ACC_ATTR_NATIVE_ID);

    let data_processing_ref = get_attr_text(&chromatogram_rows, ACC_ATTR_DATA_PROCESSING_REF)
        .or_else(|| default_data_processing_ref.map(|s| s.to_string()));

    let default_array_length_attr =
        get_attr_u32(&chromatogram_rows, ACC_ATTR_DEFAULT_ARRAY_LENGTH).map(|v| v as usize);

    let (cv_params, user_params) = parse_cv_and_user_params(&chromatogram_params_meta);

    let binary_data_array_list = parse_binary_data_array_list(metadata);

    let (x_len, y_len) = xy_lengths_from_bdal(binary_data_array_list.as_ref());
    let default_array_length: Option<usize> =
        default_array_length_attr.or(x_len).or(y_len).or(Some(0));

    let precursor = parse_precursor_for_chromatogram(metadata, chromatogram_id, children_lookup);
    let product = parse_product_for_chromatogram(metadata, chromatogram_id, children_lookup);

    Chromatogram {
        id,
        native_id,
        index,
        default_array_length,
        data_processing_ref,
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
        precursor,
        product,
        binary_data_array_list,
    }
}

/// <precursor>
#[inline]
fn parse_precursor_for_chromatogram(
    metadata: &[&Metadatum],
    chromatogram_id: u32,
    children_lookup: &ChildrenLookup,
) -> Option<Precursor> {
    let precursor_id = children_lookup.first_id(chromatogram_id, TagId::Precursor)?;

    let mut precursor_rows: Vec<&Metadatum> = Vec::new();
    for &m in metadata {
        if m.tag_id == TagId::Precursor && m.id == precursor_id {
            precursor_rows.push(m);
        }
    }

    let spectrum_ref = get_attr_text(&precursor_rows, ACC_ATTR_SPECTRUM_REF);
    let source_file_ref = get_attr_text(&precursor_rows, ACC_ATTR_SOURCE_FILE_REF);
    let external_spectrum_id = get_attr_text(&precursor_rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID);

    Some(Precursor {
        spectrum_ref,
        source_file_ref,
        external_spectrum_id,
        isolation_window: parse_isolation_window(metadata, precursor_id, children_lookup),
        selected_ion_list: parse_selected_ion_list(metadata, precursor_id, children_lookup),
        activation: parse_activation(metadata, precursor_id, children_lookup),
    })
}

/// <product>
#[inline]
fn parse_product_for_chromatogram(
    metadata: &[&Metadatum],
    chromatogram_id: u32,
    children_lookup: &ChildrenLookup,
) -> Option<Product> {
    let product_id = children_lookup.first_id(chromatogram_id, TagId::Product)?;

    Some(Product {
        spectrum_ref: None,
        source_file_ref: None,
        external_spectrum_id: None,
        isolation_window: parse_isolation_window(metadata, product_id, children_lookup),
    })
}

/// <isolationWindow>
#[inline]
fn parse_isolation_window(
    metadata: &[&Metadatum],
    parent_id: u32,
    children_lookup: &ChildrenLookup,
) -> Option<IsolationWindow> {
    let isolation_id = children_lookup.first_id(parent_id, TagId::IsolationWindow)?;

    let mut iso_params_meta: Vec<&Metadatum> = Vec::new();
    for &m in metadata {
        if m.parent_index == isolation_id
            && (m.tag_id == TagId::CvParam || m.tag_id == TagId::UserParam)
        {
            iso_params_meta.push(m);
        }
    }

    let (cv_params, user_params) = parse_cv_and_user_params(&iso_params_meta);

    Some(IsolationWindow {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}

/// <selectedIonList>
#[inline]
fn parse_selected_ion_list(
    metadata: &[&Metadatum],
    precursor_id: u32,
    children_lookup: &ChildrenLookup,
) -> Option<SelectedIonList> {
    let list_id = children_lookup.first_id(precursor_id, TagId::SelectedIonList)?;

    let mut list_rows: Vec<&Metadatum> = Vec::new();
    for &m in metadata {
        if m.tag_id == TagId::SelectedIonList && m.id == list_id {
            list_rows.push(m);
        }
    }

    let count_attr = get_attr_u32(&list_rows, ACC_ATTR_COUNT).map(|v| v as usize);

    let ion_ids = children_lookup.get_children_with_tag(list_id, TagId::SelectedIon);

    let mut selected_ions: Vec<SelectedIon> = Vec::with_capacity(ion_ids.len());
    let mut seen: HashSet<u32> = HashSet::with_capacity(ion_ids.len());

    for &ion_id in ion_ids {
        if !seen.insert(ion_id) {
            continue;
        }

        let mut ion_params_meta: Vec<&Metadatum> = Vec::new();
        for &m in metadata {
            if m.parent_index == ion_id
                && (m.tag_id == TagId::CvParam || m.tag_id == TagId::UserParam)
            {
                ion_params_meta.push(m);
            }
        }

        let (cv_params, user_params) = parse_cv_and_user_params(&ion_params_meta);

        selected_ions.push(SelectedIon {
            referenceable_param_group_refs: Vec::new(),
            cv_params,
            user_params,
        });
    }

    Some(SelectedIonList {
        count: count_attr.or(Some(selected_ions.len())),
        selected_ions,
    })
}

/// <activation>
#[inline]
fn parse_activation(
    metadata: &[&Metadatum],
    precursor_id: u32,
    children_lookup: &ChildrenLookup,
) -> Option<Activation> {
    let activation_id = children_lookup.first_id(precursor_id, TagId::Activation)?;

    let mut activation_params_meta: Vec<&Metadatum> = Vec::new();
    for &m in metadata {
        if m.parent_index == activation_id
            && (m.tag_id == TagId::CvParam || m.tag_id == TagId::UserParam)
        {
            activation_params_meta.push(m);
        }
    }

    let (cv_params, user_params) = parse_cv_and_user_params(&activation_params_meta);

    Some(Activation {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}
