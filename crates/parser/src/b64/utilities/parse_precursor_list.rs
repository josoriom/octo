use std::collections::{HashMap, HashSet};

use crate::{
    Activation, IsolationWindow, Precursor, PrecursorList, SelectedIon, SelectedIonList,
    b64::utilities::{
        common::{
            ChildIndex, OwnerRows, ParseCtx, child_params_for_parent, get_attr_text,
            ordered_unique_owner_ids, rows_for_owner, unique_ids,
        },
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
fn params_for_tagged_owner<'a>(
    owner_rows: &OwnerRows<'a>,
    child_index: &ChildIndex,
    owner_id: u32,
    tag: TagId,
) -> Vec<&'a Metadatum> {
    let mut out: Vec<&'a Metadatum> = Vec::new();

    if let Some(xs) = owner_rows.get(&owner_id) {
        for &m in xs {
            if m.tag_id == tag {
                out.push(m);
            }
        }
    }

    let mut child_meta = child_params_for_parent(owner_rows, child_index, owner_id);
    if !child_meta.is_empty() {
        out.reserve(child_meta.len());
        out.append(&mut child_meta);
    }

    for &id in child_index.ids(owner_id, TagId::ReferenceableParamGroupRef) {
        if let Some(xs) = owner_rows.get(&id) {
            out.extend(xs.iter().copied());
        }
    }

    out
}

/// <precursorList>
#[inline]
pub fn parse_precursor_list(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<PrecursorList> {
    let mut owner_rows: OwnerRows<'_> = HashMap::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);

        match m.tag_id {
            TagId::PrecursorList => {
                if list_id.is_none() {
                    list_id = Some(m.owner_id);
                }
            }
            TagId::Precursor => {
                if fallback_list_id.is_none() {
                    fallback_list_id = Some(m.parent_index);
                }
            }
            _ => {}
        }
    }

    let ctx = ParseCtx {
        metadata,
        child_index,
        owner_rows: &owner_rows,
    };

    let mut precursor_ids: Vec<u32> = Vec::new();

    if let Some(pid) = list_id.or(fallback_list_id) {
        let direct = ctx.child_index.ids(pid, TagId::Precursor);
        if !direct.is_empty() {
            precursor_ids = unique_ids(direct);
        }
    }

    if precursor_ids.is_empty() {
        precursor_ids = ordered_unique_owner_ids(metadata, TagId::Precursor);
    }

    if precursor_ids.is_empty() {
        let mut set: HashSet<u32> = HashSet::new();
        for &m in metadata {
            match m.tag_id {
                TagId::IsolationWindow | TagId::SelectedIon | TagId::Activation => {
                    if m.parent_index != 0 {
                        set.insert(m.parent_index);
                    }
                }
                _ => {}
            }
        }
        if !set.is_empty() {
            precursor_ids = set.into_iter().collect();
            precursor_ids.sort_unstable();
        }
    }

    if precursor_ids.is_empty() {
        return None;
    }

    let mut precursors = Vec::with_capacity(precursor_ids.len());
    for precursor_id in precursor_ids {
        precursors.push(parse_precursor(&ctx, precursor_id));
    }

    Some(PrecursorList {
        count: Some(precursors.len()),
        cv_params: Vec::new(),
        user_params: Vec::new(),
        precursors,
    })
}

/// <precursor>
#[inline]
fn parse_precursor(ctx: &ParseCtx<'_>, precursor_id: u32) -> Precursor {
    let rows = rows_for_owner(ctx.owner_rows, precursor_id);
    let precursor_parent = rows.first().map(|m| m.parent_index).unwrap_or(0);

    let spectrum_ref = get_attr_text(rows, ACC_ATTR_SPECTRUM_REF);
    let source_file_ref = get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF);
    let external_spectrum_id = get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID);

    let isolation_window = parse_isolation_window(ctx, precursor_id, precursor_parent);
    let selected_ion_list = parse_selected_ion_list(ctx, precursor_id, precursor_parent);
    let activation = parse_activation(ctx, precursor_id, precursor_parent);

    Precursor {
        spectrum_ref,
        source_file_ref,
        external_spectrum_id,
        isolation_window,
        selected_ion_list,
        activation,
    }
}

/// <isolationWindow>
#[inline]
fn parse_isolation_window(
    ctx: &ParseCtx<'_>,
    precursor_id: u32,
    precursor_parent: u32,
) -> Option<IsolationWindow> {
    let isolation_id = ctx
        .child_index
        .first_id(precursor_id, TagId::IsolationWindow)
        .or_else(|| {
            ctx.child_index
                .first_id(precursor_parent, TagId::IsolationWindow)
        });

    let mut meta: Vec<&Metadatum> = Vec::new();

    if let Some(iw_id) = isolation_id {
        meta = params_for_tagged_owner(
            ctx.owner_rows,
            ctx.child_index,
            iw_id,
            TagId::IsolationWindow,
        );
    }
    if meta.is_empty() {
        meta = params_for_tagged_owner(
            ctx.owner_rows,
            ctx.child_index,
            precursor_id,
            TagId::IsolationWindow,
        );
    }
    if meta.is_empty() && precursor_parent != 0 && precursor_parent != precursor_id {
        meta = params_for_tagged_owner(
            ctx.owner_rows,
            ctx.child_index,
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

/// <selectedIonList>
#[inline]
fn parse_selected_ion_list(
    ctx: &ParseCtx<'_>,
    precursor_id: u32,
    precursor_parent: u32,
) -> Option<SelectedIonList> {
    let mut combined: Vec<u32> = Vec::new();

    if let Some(list_id) = ctx
        .child_index
        .first_id(precursor_id, TagId::SelectedIonList)
    {
        combined.extend_from_slice(ctx.child_index.ids(list_id, TagId::SelectedIon));
    }
    combined.extend_from_slice(ctx.child_index.ids(precursor_id, TagId::SelectedIon));

    if combined.is_empty() {
        if let Some(list_id) = ctx
            .child_index
            .first_id(precursor_parent, TagId::SelectedIonList)
        {
            combined.extend_from_slice(ctx.child_index.ids(list_id, TagId::SelectedIon));
        }
    }
    if combined.is_empty() {
        combined.extend_from_slice(ctx.child_index.ids(precursor_parent, TagId::SelectedIon));
    }

    if combined.is_empty() {
        return None;
    }

    let mut selected_ion_ids = unique_ids(&combined);
    selected_ion_ids.sort_unstable();

    let mut selected_ions = Vec::with_capacity(selected_ion_ids.len());
    for sid in selected_ion_ids {
        let meta =
            params_for_tagged_owner(ctx.owner_rows, ctx.child_index, sid, TagId::SelectedIon);
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

/// <activation>
#[inline]
fn parse_activation(
    ctx: &ParseCtx<'_>,
    precursor_id: u32,
    precursor_parent: u32,
) -> Option<Activation> {
    let activation_id = ctx
        .child_index
        .first_id(precursor_id, TagId::Activation)
        .or_else(|| {
            ctx.child_index
                .first_id(precursor_parent, TagId::Activation)
        });

    let mut meta: Vec<&Metadatum> = Vec::new();

    if let Some(act_id) = activation_id {
        meta = params_for_tagged_owner(ctx.owner_rows, ctx.child_index, act_id, TagId::Activation);
    }
    if meta.is_empty() {
        meta = params_for_tagged_owner(
            ctx.owner_rows,
            ctx.child_index,
            precursor_id,
            TagId::Activation,
        );
    }
    if meta.is_empty() && precursor_parent != 0 && precursor_parent != precursor_id {
        meta = params_for_tagged_owner(
            ctx.owner_rows,
            ctx.child_index,
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
