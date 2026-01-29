use std::collections::HashMap;

use crate::{
    b64::utilities::{
        common::{
            ChildIndex, OwnerRows, ParseCtx, b000_attr_text, child_params_for_parent,
            ids_for_parent, rows_for_owner,
        },
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_COUNT, ACC_ATTR_ID},
        schema::TagId,
        structs::{ReferenceableParamGroup, ReferenceableParamGroupList},
    },
};

/// <referenceableParamGroupList>
#[inline]
pub fn parse_referenceable_param_group_list(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<ReferenceableParamGroupList> {
    let mut owner_rows: OwnerRows<'_> = HashMap::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);

        match m.tag_id {
            TagId::ReferenceableParamGroupList => {
                if list_id.is_none() {
                    list_id = Some(m.owner_id);
                }
            }
            TagId::ReferenceableParamGroup => {
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

    let group_ids = ids_for_parent(&ctx, list_id, TagId::ReferenceableParamGroup);
    if group_ids.is_empty() {
        return None;
    }

    let mut referenceable_param_groups = Vec::with_capacity(group_ids.len());
    for group_id in group_ids {
        referenceable_param_groups.push(parse_referenceable_param_group(&ctx, group_id));
    }

    let count = b000_attr_text(rows_for_owner(ctx.owner_rows, list_id), ACC_ATTR_COUNT)
        .and_then(|s| s.parse::<usize>().ok())
        .or(Some(referenceable_param_groups.len()));

    Some(ReferenceableParamGroupList {
        count,
        referenceable_param_groups,
    })
}

/// <referenceableParamGroup>
#[inline]
fn parse_referenceable_param_group(ctx: &ParseCtx<'_>, group_id: u32) -> ReferenceableParamGroup {
    let rows = rows_for_owner(ctx.owner_rows, group_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, group_id);

    let (cv_params, user_params) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    ReferenceableParamGroup {
        id,
        cv_params,
        user_params,
    }
}
