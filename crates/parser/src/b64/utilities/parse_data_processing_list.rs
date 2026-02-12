use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::b000_attr_text,
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

#[inline]
pub fn parse_data_processing_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<DataProcessingList> {
    let mut owner_rows: OwnerRows<'_> = OwnerRows::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    for &m in metadata {
        owner_rows.entry(m.id).or_default().push(m);
        if list_id.is_none() && m.tag_id == TagId::DataProcessingList {
            list_id = Some(m.id);
        }
    }

    let data_processing_ids = if let Some(list_id) = list_id {
        let ids = children_lookup.ids_for(metadata, list_id, TagId::DataProcessing);
        if ids.is_empty() {
            ChildrenLookup::all_ids(metadata, TagId::DataProcessing)
        } else {
            ids
        }
    } else {
        ChildrenLookup::all_ids(metadata, TagId::DataProcessing)
    };

    if data_processing_ids.is_empty() {
        return None;
    }

    let mut data_processing = Vec::with_capacity(data_processing_ids.len());
    for id in data_processing_ids {
        data_processing.push(parse_data_processing(
            metadata,
            children_lookup,
            &owner_rows,
            id,
        ));
    }

    Some(DataProcessingList {
        count: Some(data_processing.len()),
        data_processing,
    })
}

#[inline]
fn parse_data_processing<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    data_processing_id: u32,
) -> DataProcessing {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, data_processing_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let software_ref = b000_attr_text(rows, ACC_ATTR_SOFTWARE_REF).filter(|s| !s.is_empty());

    let processing_method =
        parse_processing_methods(metadata, children_lookup, owner_rows, data_processing_id);

    DataProcessing {
        id,
        software_ref,
        processing_method,
    }
}

#[inline]
fn parse_processing_methods<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    data_processing_id: u32,
) -> Vec<ProcessingMethod> {
    let ids = children_lookup.ids_for(metadata, data_processing_id, TagId::ProcessingMethod);
    if ids.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(parse_processing_method(
            metadata,
            children_lookup,
            owner_rows,
            id,
        ));
    }
    out
}

#[inline]
fn parse_processing_method<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    processing_method_id: u32,
) -> ProcessingMethod {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, processing_method_id);

    let order = b000_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse::<u32>().ok());
    let software_ref = b000_attr_text(rows, ACC_ATTR_SOFTWARE_REF).filter(|s| !s.is_empty());

    let referenceable_param_group_ref = parse_referenceable_param_group_refs(
        metadata,
        children_lookup,
        owner_rows,
        processing_method_id,
    );

    let child_meta = children_lookup.param_rows(metadata, owner_rows, processing_method_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_meta.len());
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

#[inline]
fn parse_referenceable_param_group_refs<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    let ref_ids = children_lookup.ids_for(metadata, parent_id, TagId::ReferenceableParamGroupRef);
    if ref_ids.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(ref_ids.len());
    for ref_id in ref_ids {
        let ref_rows = ChildrenLookup::rows_for_owner(owner_rows, ref_id);
        if let Some(r) = b000_attr_text(ref_rows, ACC_ATTR_REF) {
            if !r.is_empty() {
                out.push(ReferenceableParamGroupRef { r#ref: r });
            }
        }
    }
    out
}
