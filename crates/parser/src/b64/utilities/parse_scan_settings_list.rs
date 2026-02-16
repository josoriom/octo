use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF, ACC_ATTR_REF},
        schema::TagId,
        structs::{
            ReferenceableParamGroupRef, ScanSettings, ScanSettingsList, SourceFileRef,
            SourceFileRefList, Target, TargetList,
        },
    },
};
use hashbrown::HashSet;

#[inline]
pub fn parse_scan_settings_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<ScanSettingsList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let list_id = children_lookup
        .all_ids(TagId::ScanSettingsList)
        .first()
        .or_else(|| {
            children_lookup
                .all_ids(TagId::AcquisitionSettingsList)
                .first()
        })
        .copied();

    let mut ids = Vec::new();
    if let Some(id) = list_id {
        ids.extend(children_lookup.ids_for(id, TagId::ScanSettings));
        ids.extend(children_lookup.ids_for(id, TagId::AcquisitionSettings));
    }

    if ids.is_empty() {
        let mut seen = HashSet::with_capacity(metadata.len().min(1024));
        for m in metadata {
            if matches!(m.tag_id, TagId::ScanSettings | TagId::AcquisitionSettings)
                && seen.insert(m.id)
            {
                ids.push(m.id);
            }
        }
    }

    if ids.is_empty() {
        return None;
    }

    let scan_settings: Vec<_> = ids
        .into_iter()
        .map(|id| parse_scan_settings(children_lookup, &owner_rows, id))
        .collect();

    Some(ScanSettingsList {
        count: Some(scan_settings.len()),
        scan_settings,
    })
}

#[inline]
fn parse_scan_settings<'m>(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    scan_settings_id: u32,
) -> ScanSettings {
    let rows = owner_rows.get(scan_settings_id);
    let param_rows = &children_lookup.get_param_rows(owner_rows, scan_settings_id);
    let (cv_params, user_params) = parse_cv_and_user_params(&param_rows);

    ScanSettings {
        id: get_attr_text(rows, ACC_ATTR_ID).filter(|s| !s.is_empty()),
        instrument_configuration_ref: get_attr_text(rows, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF)
            .filter(|s| !s.is_empty()),
        referenceable_param_group_refs: parse_referenceable_param_group_refs(
            children_lookup,
            owner_rows,
            scan_settings_id,
        ),
        cv_params,
        user_params,
        source_file_ref_list: parse_source_file_ref_list(
            children_lookup,
            owner_rows,
            scan_settings_id,
        ),
        target_list: parse_target_list(children_lookup, owner_rows, scan_settings_id),
    }
}

#[inline]
fn parse_source_file_ref_list<'m>(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    scan_settings_id: u32,
) -> Option<SourceFileRefList> {
    for tag in [TagId::SourceFileRefList, TagId::SourceFileList] {
        if let Some(&list_id) = children_lookup.ids_for(scan_settings_id, tag).first() {
            let child_tag = if tag == TagId::SourceFileRefList {
                TagId::SourceFileRef
            } else {
                TagId::SourceFile
            };
            let attr = if tag == TagId::SourceFileRefList {
                ACC_ATTR_REF
            } else {
                ACC_ATTR_ID
            };

            let refs: Vec<_> = children_lookup
                .ids_for(list_id, child_tag)
                .iter()
                .filter_map(|&id| {
                    get_attr_text(owner_rows.get(id), attr)
                        .filter(|s| !s.is_empty())
                        .map(|r| SourceFileRef { r#ref: r })
                })
                .collect();

            if !refs.is_empty() {
                return Some(SourceFileRefList {
                    count: Some(refs.len()),
                    source_file_refs: refs,
                });
            }
        }
    }
    None
}

#[inline]
fn parse_target_list<'m>(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    scan_settings_id: u32,
) -> Option<TargetList> {
    let list_id = children_lookup
        .ids_for(scan_settings_id, TagId::TargetList)
        .first()
        .copied();
    let target_ids = list_id
        .map(|id| children_lookup.ids_for(id, TagId::Target))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.ids_for(scan_settings_id, TagId::Target));

    if !target_ids.is_empty() {
        let targets = target_ids
            .into_iter()
            .map(|id| parse_target(children_lookup, owner_rows, id))
            .collect::<Vec<_>>();
        return Some(TargetList {
            count: Some(targets.len()),
            targets,
        });
    }

    let param_rows = &children_lookup.get_param_rows(owner_rows, scan_settings_id);
    let (all_cvs, _) = parse_cv_and_user_params(&param_rows);

    let mut targets = Vec::new();
    let mut current_group = Vec::new();

    for p in all_cvs {
        let acc = p.accession.as_deref().unwrap_or("");
        if !["1000827", "1001225", "1000502", "1000747"]
            .iter()
            .any(|&s| acc.ends_with(s))
        {
            continue;
        }

        if acc.ends_with("1000827") && !current_group.is_empty() {
            targets.push(Target {
                referenceable_param_group_refs: Vec::new(),
                cv_params: std::mem::take(&mut current_group),
                user_params: Vec::new(),
            });
        }
        current_group.push(p);
    }

    if !current_group.is_empty() {
        targets.push(Target {
            referenceable_param_group_refs: Vec::new(),
            cv_params: current_group,
            user_params: Vec::new(),
        });
    }

    (!targets.is_empty()).then(|| TargetList {
        count: Some(targets.len()),
        targets,
    })
}

#[inline]
fn parse_target<'m>(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    target_id: u32,
) -> Target {
    let param_rows = &children_lookup.get_param_rows(owner_rows, target_id);
    let (cv_params, user_params) = parse_cv_and_user_params(&param_rows);

    Target {
        referenceable_param_group_refs: parse_referenceable_param_group_refs(
            children_lookup,
            owner_rows,
            target_id,
        ),
        cv_params,
        user_params,
    }
}

#[inline]
fn parse_referenceable_param_group_refs<'m>(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    children_lookup
        .ids_for(parent_id, TagId::ReferenceableParamGroupRef)
        .iter()
        .filter_map(|&id| {
            get_attr_text(owner_rows.get(id), ACC_ATTR_REF)
                .filter(|s| !s.is_empty())
                .map(|r| ReferenceableParamGroupRef { r#ref: r })
        })
        .collect()
}
