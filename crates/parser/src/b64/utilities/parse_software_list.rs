use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_COUNT, ACC_ATTR_ID, ACC_ATTR_NAME, ACC_ATTR_REF, ACC_ATTR_VERSION},
        cv_table,
        schema::TagId,
        structs::{Software, SoftwareList, SoftwareParam},
    },
};

use std::borrow::Cow;

#[inline]
pub fn parse_software_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<SoftwareList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let list_id = children_lookup
        .all_ids(TagId::SoftwareList)
        .first()
        .copied();

    let software_ids = if let Some(id) = list_id {
        let direct = children_lookup.ids_for(id, TagId::Software);
        if direct.is_empty() {
            Cow::Borrowed(children_lookup.all_ids(TagId::Software))
        } else {
            Cow::Owned(direct)
        }
    } else {
        Cow::Borrowed(children_lookup.all_ids(TagId::Software))
    };

    if software_ids.is_empty() {
        return None;
    }

    let count = list_id.and_then(|id| {
        get_attr_text(owner_rows.get(id), ACC_ATTR_COUNT).and_then(|c| c.parse::<usize>().ok())
    });

    let software = software_ids
        .iter()
        .map(|&id| parse_software(children_lookup, &owner_rows, id))
        .collect();

    Some(SoftwareList {
        count: count.or(Some(software_ids.len())),
        software,
    })
}

#[inline]
fn parse_software(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    software_id: u32,
) -> Software {
    let rows = owner_rows.get(software_id);

    let id = get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let version = get_attr_text(rows, ACC_ATTR_VERSION).filter(|s| !s.is_empty());

    let param_rows = &children_lookup.get_param_rows(owner_rows, software_id);
    let (cv_param, _) = parse_cv_and_user_params(&param_rows);

    let software_param =
        parse_software_params(children_lookup, owner_rows, software_id, version.as_deref());

    Software {
        id,
        version,
        software_param,
        cv_param,
    }
}

#[inline]
fn parse_software_params(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    software_id: u32,
    parent_version: Option<&str>,
) -> Vec<SoftwareParam> {
    children_lookup
        .ids_for(software_id, TagId::SoftwareParam)
        .iter()
        .map(|&id| parse_software_param(owner_rows, id, parent_version))
        .collect()
}

#[inline]
fn parse_software_param(
    owner_rows: &OwnerRows,
    software_param_id: u32,
    parent_version: Option<&str>,
) -> SoftwareParam {
    let rows = owner_rows.get(software_param_id);

    let version = get_attr_text(rows, ACC_ATTR_VERSION)
        .filter(|s| !s.is_empty())
        .or_else(|| parent_version.map(ToString::to_string));

    let (cv_params, _) = parse_cv_and_user_params(rows);

    if let Some(cv) = cv_params.into_iter().next() {
        let accession = cv.accession.unwrap_or_default();
        let cv_ref = cv
            .cv_ref
            .or_else(|| get_attr_text(rows, ACC_ATTR_REF).filter(|s| !s.is_empty()));

        return SoftwareParam {
            cv_ref,
            accession,
            name: cv.name,
            version,
        };
    }

    let accession = get_accession_from_rows(rows).unwrap_or_default();

    let cv_ref = get_attr_text(rows, ACC_ATTR_REF)
        .or_else(|| get_attr_text(rows, ACC_ATTR_REF))
        .filter(|s| !s.is_empty());

    let name = get_attr_text(rows, ACC_ATTR_NAME)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            cv_table::get(&accession)
                .and_then(|v| v.as_str())
                .unwrap_or(&accession)
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
fn get_accession_from_rows(rows: &[&Metadatum]) -> Option<String> {
    rows.iter().find_map(|m| {
        let acc = m.accession.as_deref()?;
        if !acc.starts_with("B000:") && acc.contains(':') {
            Some(acc.to_string())
        } else {
            None
        }
    })
}
