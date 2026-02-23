use crate::{
    Activation, IsolationWindow, Precursor, PrecursorList, SelectedIon, SelectedIonList,
    b64::utilities::{
        children_lookup::{ChildrenLookup, DefaultMetadataPolicy, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decoder::decode::Metadatum,
    mzml::{
        attr_meta::{
            ACC_ATTR_EXTERNAL_SPECTRUM_ID, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPECTRUM_REF,
        },
        schema::TagId,
    },
};

#[inline]
pub fn parse_precursor_list<'a>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
) -> Option<PrecursorList> {
    let description_id = children_lookup
        .ids_for(spectrum_id, TagId::SpectrumDescription)
        .first()
        .copied();

    let precursor_list_id = children_lookup
        .ids_for(spectrum_id, TagId::PrecursorList)
        .first()
        .copied()
        .or_else(|| {
            description_id.and_then(|id| {
                children_lookup
                    .ids_for(id, TagId::PrecursorList)
                    .first()
                    .copied()
            })
        });

    let precursor_container_id = precursor_list_id.or(description_id).unwrap_or(spectrum_id);
    let precursor_ids = children_lookup.ids_for(precursor_container_id, TagId::Precursor);
    if precursor_ids.is_empty() {
        return None;
    }

    let policy = DefaultMetadataPolicy;
    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let precursors = precursor_ids
        .iter()
        .map(|&precursor_id| {
            let rows = owner_rows.get(precursor_id);
            Precursor {
                spectrum_ref: get_attr_text(rows, ACC_ATTR_SPECTRUM_REF),
                source_file_ref: get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF),
                external_spectrum_id: get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID),
                isolation_window: parse_isolation_window(
                    owner_rows,
                    children_lookup,
                    precursor_id,
                    &policy,
                    &mut param_buffer,
                ),
                selected_ion_list: parse_selected_ion_list(
                    owner_rows,
                    children_lookup,
                    precursor_id,
                    &policy,
                    &mut param_buffer,
                ),
                activation: parse_activation(
                    owner_rows,
                    children_lookup,
                    precursor_id,
                    &policy,
                    &mut param_buffer,
                ),
            }
        })
        .collect::<Vec<_>>();

    let (cv_params, user_params) = precursor_list_id
        .map(|id| {
            param_buffer.clear();
            children_lookup.get_param_rows_into(owner_rows, id, &policy, &mut param_buffer);
            parse_cv_and_user_params(&param_buffer)
        })
        .unwrap_or_default();

    Some(PrecursorList {
        count: Some(precursors.len()),
        cv_params,
        user_params,
        precursors,
    })
}

#[inline]
fn parse_isolation_window<'a>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    precursor_id: u32,
    policy: &DefaultMetadataPolicy,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Option<IsolationWindow> {
    let isolation_window_id = children_lookup
        .ids_for(precursor_id, TagId::IsolationWindow)
        .first()
        .copied()?;

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, isolation_window_id, policy, param_buffer);
    let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);

    Some(IsolationWindow {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}

#[inline]
fn parse_selected_ion_list<'a>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    precursor_id: u32,
    policy: &DefaultMetadataPolicy,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Option<SelectedIonList> {
    let selected_ion_list_id = children_lookup
        .ids_for(precursor_id, TagId::SelectedIonList)
        .first()
        .copied();

    let selected_ion_ids = children_lookup.ids_for(
        selected_ion_list_id.unwrap_or(precursor_id),
        TagId::SelectedIon,
    );

    if selected_ion_ids.is_empty() {
        return None;
    }

    let selected_ions = selected_ion_ids
        .iter()
        .map(|&selected_ion_id| {
            param_buffer.clear();
            children_lookup.get_param_rows_into(owner_rows, selected_ion_id, policy, param_buffer);
            let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);

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
fn parse_activation<'a>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    precursor_id: u32,
    policy: &DefaultMetadataPolicy,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Option<Activation> {
    let activation_id = children_lookup
        .ids_for(precursor_id, TagId::Activation)
        .first()
        .copied()?;

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, activation_id, policy, param_buffer);
    let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);

    Some(Activation {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}
