use std::collections::HashSet;

use crate::{
    Activation, IsolationWindow, Precursor, PrecursorList, SelectedIon, SelectedIonList,
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{
            ACC_ATTR_EXTERNAL_SPECTRUM_ID, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPECTRUM_REF,
        },
        schema::TagId,
    },
};

#[inline]
fn dedup_ids(ids: Vec<u32>) -> Vec<u32> {
    let mut out = Vec::with_capacity(ids.len());
    let mut seen = HashSet::with_capacity(ids.len());
    for id in ids {
        if seen.insert(id) {
            out.push(id);
        }
    }
    out
}

#[inline]
fn params_for_tagged_owner<'m>(
    metadata: &[&'m Metadatum],
    owner_rows: &OwnerRows<'m>,
    children_lookup: &ChildrenLookup,
    id: u32,
    tag: TagId,
) -> Vec<&'m Metadatum> {
    let mut out = Vec::new();

    if let Some(xs) = owner_rows.get(&id) {
        out.extend(xs.iter().copied().filter(|m| m.tag_id == tag));
    }

    let child_meta = children_lookup.param_rows(metadata, owner_rows, id);
    if !child_meta.is_empty() {
        out.reserve(child_meta.len());
        out.extend(child_meta);
    }

    for id in children_lookup.ids_for(metadata, id, TagId::ReferenceableParamGroupRef) {
        if let Some(xs) = owner_rows.get(&id) {
            out.extend(xs.iter().copied());
        }
    }

    out
}

#[inline]
pub fn parse_precursor_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<PrecursorList> {
    let mut owner_rows: OwnerRows<'_> = OwnerRows::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.id).or_default().push(m);

        match m.tag_id {
            TagId::PrecursorList => {
                if list_id.is_none() {
                    list_id = Some(m.id);
                }
            }
            TagId::Precursor => {
                if fallback_list_id.is_none() && m.parent_index != 0 {
                    fallback_list_id = Some(m.parent_index);
                }
            }
            _ => {}
        }
    }

    let mut precursor_ids = Vec::new();

    if let Some(pid) = list_id.or(fallback_list_id) {
        precursor_ids = children_lookup.ids_for(metadata, pid, TagId::Precursor);
    }

    if precursor_ids.is_empty() {
        precursor_ids = ChildrenLookup::all_ids(metadata, TagId::Precursor);
    }

    if precursor_ids.is_empty() {
        let mut parents = HashSet::new();
        for &m in metadata {
            match m.tag_id {
                TagId::IsolationWindow | TagId::SelectedIon | TagId::Activation => {
                    if m.parent_index != 0 {
                        parents.insert(m.parent_index);
                    }
                }
                _ => {}
            }
        }
        if !parents.is_empty() {
            precursor_ids = parents.into_iter().collect();
            precursor_ids.sort_unstable();
        }
    }

    if precursor_ids.is_empty() {
        return None;
    }

    let mut precursors = Vec::with_capacity(precursor_ids.len());
    for precursor_id in precursor_ids {
        precursors.push(parse_precursor(
            metadata,
            children_lookup,
            &owner_rows,
            precursor_id,
        ));
    }

    Some(PrecursorList {
        count: Some(precursors.len()),
        cv_params: Vec::new(),
        user_params: Vec::new(),
        precursors,
    })
}

#[inline]
fn parse_precursor<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    precursor_id: u32,
) -> Precursor {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, precursor_id);
    let precursor_parent = rows.first().map(|m| m.parent_index).unwrap_or(0);

    let spectrum_ref = get_attr_text(rows, ACC_ATTR_SPECTRUM_REF);
    let source_file_ref = get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF);
    let external_spectrum_id = get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID);

    let isolation_window = parse_isolation_window(
        metadata,
        children_lookup,
        owner_rows,
        precursor_id,
        precursor_parent,
    );
    let selected_ion_list = parse_selected_ion_list(
        metadata,
        children_lookup,
        owner_rows,
        precursor_id,
        precursor_parent,
    );
    let activation = parse_activation(
        metadata,
        children_lookup,
        owner_rows,
        precursor_id,
        precursor_parent,
    );

    Precursor {
        spectrum_ref,
        source_file_ref,
        external_spectrum_id,
        isolation_window,
        selected_ion_list,
        activation,
    }
}

#[inline]
fn parse_isolation_window<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    precursor_id: u32,
    precursor_parent: u32,
) -> Option<IsolationWindow> {
    let isolation_id = children_lookup
        .first_id(precursor_id, TagId::IsolationWindow)
        .or_else(|| children_lookup.first_id(precursor_parent, TagId::IsolationWindow));

    let mut meta = Vec::new();

    if let Some(iw_id) = isolation_id {
        meta = params_for_tagged_owner(
            metadata,
            owner_rows,
            children_lookup,
            iw_id,
            TagId::IsolationWindow,
        );
    }
    if meta.is_empty() {
        meta = params_for_tagged_owner(
            metadata,
            owner_rows,
            children_lookup,
            precursor_id,
            TagId::IsolationWindow,
        );
    }
    if meta.is_empty() && precursor_parent != 0 && precursor_parent != precursor_id {
        meta = params_for_tagged_owner(
            metadata,
            owner_rows,
            children_lookup,
            precursor_parent,
            TagId::IsolationWindow,
        );
    }

    if meta.is_empty() && isolation_id.is_none() {
        return None;
    }

    let (cv_params, user_params) = parse_cv_and_user_params(&meta);

    Some(IsolationWindow {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}

#[inline]
fn parse_selected_ion_list<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    precursor_id: u32,
    precursor_parent: u32,
) -> Option<SelectedIonList> {
    let mut combined = Vec::new();

    if let Some(list_id) = children_lookup.first_id(precursor_id, TagId::SelectedIonList) {
        combined.extend(children_lookup.ids_for(metadata, list_id, TagId::SelectedIon));
    }
    combined.extend(children_lookup.ids_for(metadata, precursor_id, TagId::SelectedIon));

    if combined.is_empty() {
        if let Some(list_id) = children_lookup.first_id(precursor_parent, TagId::SelectedIonList) {
            combined.extend(children_lookup.ids_for(metadata, list_id, TagId::SelectedIon));
        }
    }
    if combined.is_empty() {
        combined.extend(children_lookup.ids_for(metadata, precursor_parent, TagId::SelectedIon));
    }

    let selected_ion_ids = dedup_ids(combined);
    if selected_ion_ids.is_empty() {
        return None;
    }

    let mut selected_ions = Vec::with_capacity(selected_ion_ids.len());
    for sid in selected_ion_ids {
        let meta = params_for_tagged_owner(
            metadata,
            owner_rows,
            children_lookup,
            sid,
            TagId::SelectedIon,
        );
        let (cv_params, user_params) = parse_cv_and_user_params(&meta);

        selected_ions.push(SelectedIon {
            referenceable_param_group_refs: Vec::new(),
            cv_params,
            user_params,
        });
    }

    Some(SelectedIonList {
        count: Some(selected_ions.len()),
        selected_ions,
    })
}

#[inline]
fn parse_activation<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    precursor_id: u32,
    precursor_parent: u32,
) -> Option<Activation> {
    let activation_id = children_lookup
        .first_id(precursor_id, TagId::Activation)
        .or_else(|| children_lookup.first_id(precursor_parent, TagId::Activation));

    let mut meta = Vec::new();

    if let Some(act_id) = activation_id {
        meta = params_for_tagged_owner(
            metadata,
            owner_rows,
            children_lookup,
            act_id,
            TagId::Activation,
        );
    }
    if meta.is_empty() {
        meta = params_for_tagged_owner(
            metadata,
            owner_rows,
            children_lookup,
            precursor_id,
            TagId::Activation,
        );
    }
    if meta.is_empty() && precursor_parent != 0 && precursor_parent != precursor_id {
        meta = params_for_tagged_owner(
            metadata,
            owner_rows,
            children_lookup,
            precursor_parent,
            TagId::Activation,
        );
    }

    if meta.is_empty() && activation_id.is_none() {
        return None;
    }

    let (cv_params, user_params) = parse_cv_and_user_params(&meta);

    Some(Activation {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}
