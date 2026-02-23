use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decoder::decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_COUNT, ACC_ATTR_ID, ACC_ATTR_NAME, ACC_ATTR_REF, ACC_ATTR_VERSION},
        cv_table,
        schema::TagId,
        structs::{Software, SoftwareList, SoftwareParam},
    },
};

const EXCLUDED_ACCESSION_PREFIX: &str = "B000:";

#[inline]
pub fn parse_software_list<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<SoftwareList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
    }

    let list_id = children_lookup
        .all_ids(TagId::SoftwareList)
        .first()
        .copied();

    let software_ids: &[u32] = if let Some(id) = list_id {
        let direct = children_lookup.ids_for(id, TagId::Software);
        if direct.is_empty() {
            children_lookup.all_ids(TagId::Software)
        } else {
            direct
        }
    } else {
        children_lookup.all_ids(TagId::Software)
    };

    if software_ids.is_empty() {
        return None;
    }

    let count = list_id.and_then(|id| {
        get_attr_text(owner_rows.get(id), ACC_ATTR_COUNT).and_then(|c| c.parse::<usize>().ok())
    });

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let software = software_ids
        .iter()
        .map(|&software_id| {
            parse_software(
                children_lookup,
                &owner_rows,
                software_id,
                policy,
                &mut param_buffer,
            )
        })
        .collect();

    Some(SoftwareList {
        count: count.or(Some(software_ids.len())),
        software,
    })
}

#[inline]
fn parse_software<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    software_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Software {
    let rows = owner_rows.get(software_id);

    let id = get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let version = get_attr_text(rows, ACC_ATTR_VERSION).filter(|s| !s.is_empty());

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, software_id, policy, param_buffer);
    let (cv_param, user_params) = parse_cv_and_user_params(param_buffer);

    let software_param =
        parse_software_params(children_lookup, owner_rows, software_id, version.as_deref());

    Software {
        id,
        version,
        software_param,
        cv_param,
        user_params,
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
        .map(|&software_param_id| {
            parse_software_param(owner_rows, software_param_id, parent_version)
        })
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

    let (mut cv_params, mut user_params) = parse_cv_and_user_params(rows);

    if let Some(cv) = cv_params.pop() {
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

    if let Some(user) = user_params.pop() {
        return SoftwareParam {
            cv_ref: None,
            accession: String::new(),
            name: user.name,
            version,
        };
    }

    let accession = accession_from_rows(rows).unwrap_or_default();

    let cv_ref = get_attr_text(rows, ACC_ATTR_REF).filter(|s| !s.is_empty());

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
fn accession_from_rows(rows: &[&Metadatum]) -> Option<String> {
    rows.iter().find_map(|entry| {
        let accession = entry.accession.as_deref()?;
        if !accession.starts_with(EXCLUDED_ACCESSION_PREFIX) && accession.contains(':') {
            Some(accession.to_string())
        } else {
            None
        }
    })
}
