use crate::{
    CvParam,
    b64::utilities::{
        children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decoder::decode::Metadatum,
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

const ACC_SUFFIX_TARGET_MZ: &str = "1000827";
const ACC_SUFFIX_TARGET_NAME: &str = "1001225";
const ACC_SUFFIX_TARGET_START_MZ: &str = "1000502";
const ACC_SUFFIX_TARGET_END_MZ: &str = "1000747";

#[inline]
pub fn parse_scan_settings_list<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<ScanSettingsList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
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

    let mut scan_settings_ids = Vec::new();
    if let Some(id) = list_id {
        scan_settings_ids.extend_from_slice(children_lookup.ids_for(id, TagId::ScanSettings));
        scan_settings_ids
            .extend_from_slice(children_lookup.ids_for(id, TagId::AcquisitionSettings));
    }

    if scan_settings_ids.is_empty() {
        let mut visited = HashSet::with_capacity(metadata.len().min(1024));
        for entry in metadata {
            if matches!(
                entry.tag_id,
                TagId::ScanSettings | TagId::AcquisitionSettings
            ) && visited.insert(entry.id)
            {
                scan_settings_ids.push(entry.id);
            }
        }
    }

    if scan_settings_ids.is_empty() {
        return None;
    }

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let scan_settings: Vec<_> = scan_settings_ids
        .into_iter()
        .map(|scan_settings_id| {
            parse_scan_settings(
                children_lookup,
                &owner_rows,
                scan_settings_id,
                policy,
                &mut param_buffer,
            )
        })
        .collect();

    Some(ScanSettingsList {
        count: Some(scan_settings.len()),
        scan_settings,
    })
}

#[inline]
fn parse_scan_settings<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    scan_settings_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> ScanSettings {
    let rows = owner_rows.get(scan_settings_id);

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, scan_settings_id, policy, param_buffer);
    let (mut cv_params, user_params) = parse_cv_and_user_params(param_buffer);

    let target_list = parse_target_list(
        children_lookup,
        owner_rows,
        scan_settings_id,
        &mut cv_params,
        policy,
        param_buffer,
    );

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
        target_list,
    }
}

#[inline]
fn parse_source_file_ref_list<'a>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    scan_settings_id: u32,
) -> Option<SourceFileRefList> {
    for tag in [TagId::SourceFileRefList, TagId::SourceFileList] {
        if let Some(&list_id) = children_lookup.ids_for(scan_settings_id, tag).first() {
            let child_tag = if tag == TagId::SourceFileRefList {
                TagId::SourceFileRef
            } else {
                TagId::SourceFile
            };
            let ref_attr = if tag == TagId::SourceFileRefList {
                ACC_ATTR_REF
            } else {
                ACC_ATTR_ID
            };

            let source_file_refs: Vec<_> = children_lookup
                .ids_for(list_id, child_tag)
                .iter()
                .filter_map(|&id| {
                    get_attr_text(owner_rows.get(id), ref_attr)
                        .filter(|s| !s.is_empty())
                        .map(|r| SourceFileRef { r#ref: r })
                })
                .collect();

            if !source_file_refs.is_empty() {
                return Some(SourceFileRefList {
                    count: Some(source_file_refs.len()),
                    source_file_refs,
                });
            }
        }
    }
    None
}

#[inline]
fn parse_target_list<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    scan_settings_id: u32,
    cv_params: &mut Vec<CvParam>,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Option<TargetList> {
    let target_list_id = children_lookup
        .ids_for(scan_settings_id, TagId::TargetList)
        .first()
        .copied();

    let target_ids: &[u32] = target_list_id
        .map(|id| children_lookup.ids_for(id, TagId::Target))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.ids_for(scan_settings_id, TagId::Target));

    if !target_ids.is_empty() {
        let targets = target_ids
            .iter()
            .map(|&target_id| {
                parse_target(children_lookup, owner_rows, target_id, policy, param_buffer)
            })
            .collect::<Vec<_>>();

        return Some(TargetList {
            count: Some(targets.len()),
            targets,
        });
    }

    let targets = extract_targets_from_cv_params(cv_params);

    (!targets.is_empty()).then(|| TargetList {
        count: Some(targets.len()),
        targets,
    })
}

#[inline]
fn is_target_accession(accession: &str) -> bool {
    [
        ACC_SUFFIX_TARGET_MZ,
        ACC_SUFFIX_TARGET_NAME,
        ACC_SUFFIX_TARGET_START_MZ,
        ACC_SUFFIX_TARGET_END_MZ,
    ]
    .iter()
    .any(|&suffix| accession.ends_with(suffix))
}

#[inline]
fn extract_targets_from_cv_params(cv_params: &mut Vec<CvParam>) -> Vec<Target> {
    let mut targets: Vec<Target> = Vec::new();
    let mut current_group: Vec<CvParam> = Vec::new();
    let mut index = 0;

    while index < cv_params.len() {
        let accession = cv_params[index].accession.as_deref().unwrap_or("");
        if is_target_accession(accession) {
            let param = cv_params.remove(index);
            let is_new_target_group = param
                .accession
                .as_deref()
                .unwrap_or("")
                .ends_with(ACC_SUFFIX_TARGET_MZ);

            if is_new_target_group && !current_group.is_empty() {
                targets.push(Target {
                    referenceable_param_group_refs: Vec::new(),
                    cv_params: std::mem::take(&mut current_group),
                    user_params: Vec::new(),
                });
            }
            current_group.push(param);
        } else {
            index += 1;
        }
    }

    if !current_group.is_empty() {
        targets.push(Target {
            referenceable_param_group_refs: Vec::new(),
            cv_params: current_group,
            user_params: Vec::new(),
        });
    }

    targets
}

#[inline]
fn parse_target<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    target_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Target {
    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, target_id, policy, param_buffer);
    let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);

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
fn parse_referenceable_param_group_refs(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
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
