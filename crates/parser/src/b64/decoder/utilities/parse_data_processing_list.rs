use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decoder::decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_ORDER, ACC_ATTR_REF, ACC_ATTR_SOFTWARE_REF},
        schema::TagId,
        structs::{
            DataProcessing, DataProcessingList, ProcessingMethod, ReferenceableParamGroupRef,
        },
    },
};

#[inline]
pub fn parse_data_processing_list<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<DataProcessingList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
    }

    let list_id = children_lookup
        .all_ids(TagId::DataProcessingList)
        .first()
        .copied();

    let data_processing_ids: &[u32] = if let Some(id) = list_id {
        let direct = children_lookup.ids_for(id, TagId::DataProcessing);
        if direct.is_empty() {
            children_lookup.all_ids(TagId::DataProcessing)
        } else {
            direct
        }
    } else {
        children_lookup.all_ids(TagId::DataProcessing)
    };

    if data_processing_ids.is_empty() {
        return None;
    }

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let data_processing: Vec<DataProcessing> = data_processing_ids
        .iter()
        .map(|&data_processing_id| {
            parse_data_processing(
                children_lookup,
                &owner_rows,
                data_processing_id,
                policy,
                &mut param_buffer,
            )
        })
        .collect();

    Some(DataProcessingList {
        count: Some(data_processing.len()),
        data_processing,
    })
}

#[inline]
fn parse_data_processing<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    data_processing_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> DataProcessing {
    let rows = owner_rows.get(data_processing_id);

    DataProcessing {
        id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
        software_ref: get_attr_text(rows, ACC_ATTR_SOFTWARE_REF).filter(|s| !s.is_empty()),
        processing_method: children_lookup
            .ids_for(data_processing_id, TagId::ProcessingMethod)
            .iter()
            .map(|&method_id| {
                parse_processing_method(
                    children_lookup,
                    owner_rows,
                    method_id,
                    policy,
                    param_buffer,
                )
            })
            .collect(),
    }
}

#[inline]
fn parse_processing_method<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    processing_method_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> ProcessingMethod {
    let rows = owner_rows.get(processing_method_id);

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, processing_method_id, policy, param_buffer);
    let (cv_param, user_param) = parse_cv_and_user_params(param_buffer);

    ProcessingMethod {
        order: get_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse().ok()),
        software_ref: get_attr_text(rows, ACC_ATTR_SOFTWARE_REF).filter(|s| !s.is_empty()),
        referenceable_param_group_ref: parse_referenceable_param_group_refs(
            children_lookup,
            owner_rows,
            processing_method_id,
        ),
        cv_param,
        user_param,
    }
}

#[inline]
fn parse_referenceable_param_group_refs(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    children_lookup
        .ids_for(parent_id, TagId::ReferenceableParamGroupRef)
        .iter()
        .filter_map(|&ref_id| {
            get_attr_text(owner_rows.get(ref_id), ACC_ATTR_REF)
                .filter(|s| !s.is_empty())
                .map(|r| ReferenceableParamGroupRef { r#ref: r })
        })
        .collect()
}
