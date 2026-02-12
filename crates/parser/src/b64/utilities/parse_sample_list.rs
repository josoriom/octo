use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::b000_attr_text,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_NAME, ACC_ATTR_REF},
        schema::TagId,
        structs::{ReferenceableParamGroupRef, Sample, SampleList},
    },
};

#[inline]
pub fn parse_sample_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<SampleList> {
    let mut owner_rows: OwnerRows<'_> = OwnerRows::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    for &m in metadata {
        owner_rows.entry(m.id).or_default().push(m);
        if list_id.is_none() && m.tag_id == TagId::SampleList {
            list_id = Some(m.id);
        }
    }

    let sample_ids = if let Some(list_id) = list_id {
        let ids = children_lookup.ids_for(metadata, list_id, TagId::Sample);
        if ids.is_empty() {
            ChildrenLookup::all_ids(metadata, TagId::Sample)
        } else {
            ids
        }
    } else {
        ChildrenLookup::all_ids(metadata, TagId::Sample)
    };

    if sample_ids.is_empty() {
        return None;
    }

    let mut samples = Vec::with_capacity(sample_ids.len());
    for sample_id in sample_ids {
        samples.push(parse_sample(
            metadata,
            children_lookup,
            &owner_rows,
            sample_id,
        ));
    }

    Some(SampleList {
        count: Some(samples.len() as u32),
        samples,
    })
}

#[inline]
fn parse_sample<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    sample_id: u32,
) -> Sample {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, sample_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let name = b000_attr_text(rows, ACC_ATTR_NAME).unwrap_or_default();

    let referenceable_param_group_ref = children_lookup
        .ids_for(metadata, sample_id, TagId::ReferenceableParamGroupRef)
        .first()
        .copied()
        .and_then(|ref_id| {
            b000_attr_text(
                ChildrenLookup::rows_for_owner(owner_rows, ref_id),
                ACC_ATTR_REF,
            )
            .filter(|s| !s.is_empty())
            .map(|r| ReferenceableParamGroupRef { r#ref: r })
        });

    Sample {
        id,
        name,
        referenceable_param_group_ref,
    }
}
