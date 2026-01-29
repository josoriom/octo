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
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_NAME, ACC_ATTR_REF, ACC_ATTR_VERSION},
        cv_table,
        schema::TagId,
        structs::{Software, SoftwareList, SoftwareParam},
    },
};

/// <softwareList>
#[inline]
pub fn parse_software_list(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<SoftwareList> {
    let mut owner_rows: OwnerRows<'_> = HashMap::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);

        match m.tag_id {
            TagId::SoftwareList => {
                if list_id.is_none() {
                    list_id = Some(m.owner_id);
                }
            }
            TagId::Software => {
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

    let software_ids = ids_for_parent(&ctx, list_id, TagId::Software);
    if software_ids.is_empty() {
        return None;
    }

    let mut software = Vec::with_capacity(software_ids.len());
    for id in software_ids {
        software.push(parse_software(&ctx, id));
    }

    Some(SoftwareList {
        count: Some(software.len()),
        software,
    })
}

/// <software>
#[inline]
fn parse_software(ctx: &ParseCtx<'_>, software_id: u32) -> Software {
    let rows = rows_for_owner(ctx.owner_rows, software_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let version = b000_attr_text(rows, ACC_ATTR_VERSION).filter(|s| !s.is_empty());

    let software_param = parse_software_params(ctx, software_id, version.as_deref());

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, software_id);

    let (cv_param, _) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    Software {
        id,
        version,
        software_param,
        cv_param,
    }
}

/// <softwareParam>
#[inline]
fn parse_software_params(
    ctx: &ParseCtx<'_>,
    software_id: u32,
    parent_version: Option<&str>,
) -> Vec<SoftwareParam> {
    let ids = ids_for_parent(ctx, software_id, TagId::SoftwareParam);
    if ids.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(parse_software_param(ctx, id, parent_version));
    }
    out
}

/// <softwareParam>
#[inline]
fn parse_software_param(
    ctx: &ParseCtx<'_>,
    software_param_id: u32,
    parent_version: Option<&str>,
) -> SoftwareParam {
    let rows = rows_for_owner(ctx.owner_rows, software_param_id);

    let version = b000_attr_text(rows, ACC_ATTR_VERSION)
        .filter(|s| !s.is_empty())
        .or_else(|| parent_version.map(|s| s.to_string()));

    let (cv_params, _user_params) = parse_cv_and_user_params(rows);

    if let Some(cv) = cv_params.into_iter().next() {
        let accession = cv.accession.unwrap_or_default();

        let cv_ref = b000_attr_text(rows, ACC_ATTR_REF)
            .filter(|s| !s.is_empty())
            .or(cv.cv_ref);

        return SoftwareParam {
            cv_ref,
            accession,
            name: cv.name,
            version,
        };
    }

    let accession = software_param_accession(rows).unwrap_or_default();
    let cv_ref = b000_attr_text(rows, ACC_ATTR_REF).filter(|s| !s.is_empty());

    let name = b000_attr_text(rows, ACC_ATTR_NAME)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            cv_table::get(accession.as_str())
                .and_then(|v| v.as_str())
                .unwrap_or(accession.as_str())
                .to_string()
        });

    SoftwareParam {
        cv_ref,
        accession,
        name,
        version,
    }
}

#[inline]
fn software_param_accession(rows: &[&Metadatum]) -> Option<String> {
    for m in rows {
        let acc = m.accession.as_deref()?;
        if acc.starts_with("B000:") {
            continue;
        }
        if acc.contains(':') {
            return Some(acc.to_string());
        }
    }
    None
}
