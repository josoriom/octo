use std::collections::{HashMap, HashSet};

use crate::{
    b64::utilities::{
        common::{
            ChildIndex, OwnerRows, ParseCtx, b000_attr_text, child_params_for_parent,
            ids_for_parent, rows_for_owner, unique_ids,
        },
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_ORDER, ACC_ATTR_REF, ACC_ATTR_SOFTWARE_REF},
        schema::TagId,
        structs::{
            DataProcessing, DataProcessingList, ProcessingMethod, ReferenceableParamGroupRef,
        },
    },
};

/// <dataProcessingList>
#[inline]
pub fn parse_data_processing_list(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<DataProcessingList> {
    let mut owner_rows: OwnerRows<'_> = HashMap::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);

        match m.tag_id {
            TagId::DataProcessingList => {
                if list_id.is_none() {
                    list_id = Some(m.owner_id);
                }
            }
            TagId::DataProcessing => {
                if fallback_list_id.is_none() {
                    fallback_list_id = Some(m.parent_index);
                }
            }
            _ => {}
        }
    }

    let list_id = list_id.or(fallback_list_id)?;

    let ctx = ParseCtx {
        metadata,
        child_index,
        owner_rows: &owner_rows,
    };

    let data_processing_ids = ids_for_parent(&ctx, list_id, TagId::DataProcessing);
    if data_processing_ids.is_empty() {
        return None;
    }

    let empty_allowed: HashSet<&str> = HashSet::new();

    let mut data_processing = Vec::with_capacity(data_processing_ids.len());
    for id in data_processing_ids {
        data_processing.push(parse_data_processing(&ctx, &empty_allowed, id));
    }

    Some(DataProcessingList {
        count: Some(data_processing.len()),
        data_processing,
    })
}

/// <dataProcessing>
#[inline]
fn parse_data_processing(
    ctx: &ParseCtx<'_>,
    _empty_allowed: &HashSet<&str>,
    data_processing_id: u32,
) -> DataProcessing {
    let rows = rows_for_owner(ctx.owner_rows, data_processing_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let software_ref = b000_attr_text(rows, ACC_ATTR_SOFTWARE_REF).filter(|s| !s.is_empty());

    let processing_method = parse_processing_methods(ctx, data_processing_id);

    DataProcessing {
        id,
        software_ref,
        processing_method,
    }
}

/// <processingMethod>
#[inline]
fn parse_processing_methods(ctx: &ParseCtx<'_>, data_processing_id: u32) -> Vec<ProcessingMethod> {
    let ids = ids_for_parent(ctx, data_processing_id, TagId::ProcessingMethod);
    if ids.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(parse_processing_method(ctx, id));
    }
    out
}

/// <processingMethod>
#[inline]
fn parse_processing_method(ctx: &ParseCtx<'_>, processing_method_id: u32) -> ProcessingMethod {
    let rows = rows_for_owner(ctx.owner_rows, processing_method_id);

    let order = b000_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse::<u32>().ok());
    let software_ref = b000_attr_text(rows, ACC_ATTR_SOFTWARE_REF).filter(|s| !s.is_empty());

    let referenceable_param_group_ref =
        parse_referenceable_param_group_refs(ctx.owner_rows, ctx.child_index, processing_method_id);

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, processing_method_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta: Vec<&Metadatum> = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    ProcessingMethod {
        order,
        software_ref,
        referenceable_param_group_ref,
        cv_param,
        user_param,
    }
}

/// <referenceableParamGroupRef>
#[inline]
fn parse_referenceable_param_group_refs(
    owner_rows: &HashMap<u32, Vec<&Metadatum>>,
    child_index: &ChildIndex,
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    let ref_ids = unique_ids(child_index.ids(parent_id, TagId::ReferenceableParamGroupRef));
    if ref_ids.is_empty() {
        return Vec::new();
    }

    let mut out: Vec<ReferenceableParamGroupRef> = Vec::with_capacity(ref_ids.len());
    for ref_id in ref_ids {
        let ref_rows = rows_for_owner(owner_rows, ref_id);
        if let Some(r) = b000_attr_text(ref_rows, ACC_ATTR_REF) {
            if !r.is_empty() {
                out.push(ReferenceableParamGroupRef { r#ref: r });
            }
        }
    }
    out
}
