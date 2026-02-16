use crate::{
    Chromatogram, ChromatogramList,
    b64::{
        decode::Metadatum,
        utilities::{
            children_lookup::{ChildrenLookup, OwnerRows},
            common::{get_attr_text, get_attr_u32, xy_lengths_from_bdal},
            parse_binary_data_array_list::parse_binary_data_array_list,
            parse_cv_and_user_params,
        },
    },
    mzml::{
        attr_meta::{
            ACC_ATTR_COUNT, ACC_ATTR_DATA_PROCESSING_REF, ACC_ATTR_DEFAULT_ARRAY_LENGTH,
            ACC_ATTR_DEFAULT_DATA_PROCESSING_REF, ACC_ATTR_EXTERNAL_SPECTRUM_ID, ACC_ATTR_ID,
            ACC_ATTR_INDEX, ACC_ATTR_NATIVE_ID, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPECTRUM_REF,
        },
        schema::TagId,
        structs::{Activation, IsolationWindow, Precursor, Product, SelectedIon, SelectedIonList},
    },
};

#[inline]
pub fn parse_chromatogram_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<ChromatogramList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let list_id = children_lookup
        .all_ids(TagId::ChromatogramList)
        .first()
        .copied()?;

    let chromatogram_ids = children_lookup.ids_for(list_id, TagId::Chromatogram);
    if chromatogram_ids.is_empty() {
        return None;
    }

    let list_rows = owner_rows.get(list_id);
    let count = get_attr_u32(list_rows, ACC_ATTR_COUNT).map(|v| v as usize);
    let default_dp_ref = get_attr_text(list_rows, ACC_ATTR_DEFAULT_DATA_PROCESSING_REF);

    let chromatograms = chromatogram_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            parse_chromatogram(
                &owner_rows,
                children_lookup,
                id,
                i as u32,
                default_dp_ref.as_deref(),
            )
        })
        .collect::<Vec<_>>();

    Some(ChromatogramList {
        count: count.or(Some(chromatograms.len())),
        default_data_processing_ref: default_dp_ref,
        chromatograms,
    })
}

#[inline]
fn parse_chromatogram(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    id_u32: u32,
    fallback_index: u32,
    default_dp_ref: Option<&str>,
) -> Chromatogram {
    let rows = owner_rows.get(id_u32);
    let (cv_params, user_params) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id_u32));

    let binary_data_array_list = parse_binary_data_array_list(owner_rows, children_lookup, id_u32);
    let (x_len, y_len) = xy_lengths_from_bdal(binary_data_array_list.as_ref());

    Chromatogram {
        id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
        native_id: get_attr_text(rows, ACC_ATTR_NATIVE_ID),
        index: get_attr_u32(rows, ACC_ATTR_INDEX).or(Some(fallback_index)),
        default_array_length: get_attr_u32(rows, ACC_ATTR_DEFAULT_ARRAY_LENGTH)
            .map(|v| v as usize)
            .or(x_len)
            .or(y_len)
            .or(Some(0)),
        data_processing_ref: get_attr_text(rows, ACC_ATTR_DATA_PROCESSING_REF)
            .or_else(|| default_dp_ref.map(ToString::to_string)),
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
        precursor: parse_precursor(owner_rows, children_lookup, id_u32),
        product: parse_product(owner_rows, children_lookup, id_u32),
        binary_data_array_list,
    }
}

#[inline]
fn parse_precursor(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    parent_id: u32,
) -> Option<Precursor> {
    let id = children_lookup.first_id(parent_id, TagId::Precursor)?;
    let rows = owner_rows.get(id);

    Some(Precursor {
        spectrum_ref: get_attr_text(rows, ACC_ATTR_SPECTRUM_REF),
        source_file_ref: get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF),
        external_spectrum_id: get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID),
        isolation_window: parse_isolation_window(owner_rows, children_lookup, id),
        selected_ion_list: parse_selected_ion_list(owner_rows, children_lookup, id),
        activation: parse_activation(owner_rows, children_lookup, id),
    })
}

#[inline]
fn parse_product(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    parent_id: u32,
) -> Option<Product> {
    let id = children_lookup.first_id(parent_id, TagId::Product)?;
    let rows = owner_rows.get(id);

    let (cv_params, user_params) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));

    Some(Product {
        spectrum_ref: get_attr_text(rows, ACC_ATTR_SPECTRUM_REF),
        source_file_ref: get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF),
        external_spectrum_id: get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID),
        isolation_window: parse_isolation_window(owner_rows, children_lookup, id),
        cv_params,
        user_params,
    })
}
#[inline]
fn parse_isolation_window(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    parent_id: u32,
) -> Option<IsolationWindow> {
    let id = children_lookup.first_id(parent_id, TagId::IsolationWindow)?;
    let (cv_params, user_params) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));

    Some(IsolationWindow {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}

#[inline]
fn parse_activation(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    parent_id: u32,
) -> Option<Activation> {
    let id = children_lookup.first_id(parent_id, TagId::Activation)?;
    let (cv_params, user_params) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));

    Some(Activation {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}

#[inline]
fn parse_selected_ion_list(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    precursor_id: u32,
) -> Option<SelectedIonList> {
    let list_id = children_lookup.first_id(precursor_id, TagId::SelectedIonList)?;
    let ions = children_lookup
        .ids_for(list_id, TagId::SelectedIon)
        .iter()
        .map(|&id| {
            let (cv_params, user_params) =
                parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));
            SelectedIon {
                referenceable_param_group_refs: Vec::new(),
                cv_params,
                user_params,
            }
        })
        .collect::<Vec<_>>();

    Some(SelectedIonList {
        count: Some(ions.len()),
        selected_ions: ions,
    })
}
