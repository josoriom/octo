use std::collections::HashMap;

use crate::{
    b64::utilities::common::{
        ChildIndex, OwnerRows, ParseCtx, b000_attr_text, ids_for_parent, rows_for_owner,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_NAME, ACC_ATTR_REF},
        schema::TagId,
        structs::{ReferenceableParamGroupRef, Sample, SampleList},
    },
};

/// <sampleList>
#[inline]
pub fn parse_sample_list(metadata: &[&Metadatum], child_index: &ChildIndex) -> Option<SampleList> {
    let mut owner_rows: OwnerRows<'_> = HashMap::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);

        match m.tag_id {
            TagId::SampleList => {
                if list_id.is_none() {
                    list_id = Some(m.owner_id);
                }
            }
            TagId::Sample => {
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

    let sample_ids = ids_for_parent(&ctx, list_id, TagId::Sample);
    if sample_ids.is_empty() {
        return None;
    }

    let mut samples = Vec::with_capacity(sample_ids.len());
    for sample_id in sample_ids {
        samples.push(parse_sample(&ctx, sample_id));
    }

    Some(SampleList {
        count: Some(samples.len() as u32),
        samples,
    })
}

/// <sample>
#[inline]
fn parse_sample(ctx: &ParseCtx<'_>, sample_id: u32) -> Sample {
    let rows = rows_for_owner(ctx.owner_rows, sample_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let name = b000_attr_text(rows, ACC_ATTR_NAME).unwrap_or_default();

    let mut referenceable_param_group_ref = None;

    let ref_ids = ids_for_parent(ctx, sample_id, TagId::ReferenceableParamGroupRef);
    if let Some(&ref_id) = ref_ids.first() {
        let ref_rows = rows_for_owner(ctx.owner_rows, ref_id);
        if let Some(r) = b000_attr_text(ref_rows, ACC_ATTR_REF) {
            if !r.is_empty() {
                referenceable_param_group_ref = Some(ReferenceableParamGroupRef { r#ref: r });
            }
        }
    }

    Sample {
        id,
        name,
        referenceable_param_group_ref,
    }
}
