use crate::{
    Activation, IsolationWindow, Precursor, PrecursorList, SelectedIon, SelectedIonList,
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    mzml::{
        attr_meta::{
            ACC_ATTR_EXTERNAL_SPECTRUM_ID, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPECTRUM_REF,
        },
        schema::TagId,
    },
};

#[inline]
pub fn parse_precursor_list(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
) -> Option<PrecursorList> {
    let desc_id = children_lookup.first_id(spectrum_id, TagId::SpectrumDescription);
    let list_id = children_lookup
        .first_id(spectrum_id, TagId::PrecursorList)
        .or_else(|| desc_id.and_then(|id| children_lookup.first_id(id, TagId::PrecursorList)));

    let precursor_ids =
        children_lookup.ids_for(list_id.or(desc_id).unwrap_or(spectrum_id), TagId::Precursor);
    if precursor_ids.is_empty() {
        return None;
    }

    let precursors = precursor_ids
        .iter()
        .map(|&id| {
            let rows = owner_rows.get(id);
            Precursor {
                spectrum_ref: get_attr_text(rows, ACC_ATTR_SPECTRUM_REF),
                source_file_ref: get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF),
                external_spectrum_id: get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID),
                isolation_window: parse_isolation_window(owner_rows, children_lookup, id),
                selected_ion_list: parse_selected_ion_list(owner_rows, children_lookup, id),
                activation: parse_activation(owner_rows, children_lookup, id),
            }
        })
        .collect::<Vec<_>>();

    let (cv_params, user_params) = list_id
        .map(|id| parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id)))
        .unwrap_or_default();

    Some(PrecursorList {
        count: Some(precursors.len()),
        cv_params,
        user_params,
        precursors,
    })
}

#[inline]
fn parse_isolation_window(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    precursor_id: u32,
) -> Option<IsolationWindow> {
    let id = children_lookup.first_id(precursor_id, TagId::IsolationWindow)?;
    let (cv_params, user_params) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));

    Some(IsolationWindow {
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
    let list_id = children_lookup.first_id(precursor_id, TagId::SelectedIonList);
    let ion_ids = children_lookup.ids_for(list_id.unwrap_or(precursor_id), TagId::SelectedIon);

    if ion_ids.is_empty() {
        return None;
    }

    let selected_ions = ion_ids
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
        count: Some(selected_ions.len()),
        selected_ions,
    })
}

#[inline]
fn parse_activation(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    precursor_id: u32,
) -> Option<Activation> {
    let id = children_lookup.first_id(precursor_id, TagId::Activation)?;
    let (cv_params, user_params) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));

    Some(Activation {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}
