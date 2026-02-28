use crate::{
    CvParam,
    b64::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF, ACC_ATTR_REF},
        utilities::{
            children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
            common::get_attr_text,
            parse_cv_and_user_params,
        },
    },
    decoder::decode::Metadatum,
    mzml::{
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

pub(crate) fn parse_scan_settings_list<P: MetadataPolicy>(
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

fn parse_source_file_ref_list(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
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

fn extract_targets_from_cv_params(cv_params: &mut Vec<CvParam>) -> Vec<Target> {
    let original_len = cv_params.len();
    let mut retained: Vec<CvParam> = Vec::with_capacity(original_len);
    let mut target_params: Vec<CvParam> = Vec::new();

    for param in cv_params.drain(..) {
        if is_target_accession(param.accession.as_deref().unwrap_or("")) {
            target_params.push(param);
        } else {
            retained.push(param);
        }
    }
    *cv_params = retained;

    if target_params.is_empty() {
        return Vec::new();
    }

    let mut targets: Vec<Target> = Vec::new();
    let mut current_group: Vec<CvParam> = Vec::new();

    for param in target_params {
        let opens_new_target = param
            .accession
            .as_deref()
            .unwrap_or("")
            .ends_with(ACC_SUFFIX_TARGET_MZ);

        if opens_new_target && !current_group.is_empty() {
            targets.push(make_target(std::mem::take(&mut current_group)));
        }
        current_group.push(param);
    }

    if !current_group.is_empty() {
        targets.push(make_target(current_group));
    }

    targets
}

fn make_target(cv_params: Vec<CvParam>) -> Target {
    Target {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params: Vec::new(),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    fn make_cv_param(accession: &str) -> CvParam {
        CvParam {
            cv_ref: Some("MS".to_string()),
            accession: Some(accession.to_string()),
            name: String::new(),
            value: None,
            unit_cv_ref: None,
            unit_name: None,
            unit_accession: None,
        }
    }

    #[test]
    fn is_target_accession_recognises_all_target_suffixes() {
        assert!(is_target_accession(&format!("MS:{ACC_SUFFIX_TARGET_MZ}")));
        assert!(is_target_accession(&format!("MS:{ACC_SUFFIX_TARGET_NAME}")));
        assert!(is_target_accession(&format!(
            "MS:{ACC_SUFFIX_TARGET_START_MZ}"
        )));
        assert!(is_target_accession(&format!(
            "MS:{ACC_SUFFIX_TARGET_END_MZ}"
        )));
    }

    #[test]
    fn is_target_accession_rejects_unrelated_accession() {
        assert!(!is_target_accession("MS:1000511"));
        assert!(!is_target_accession(""));
        assert!(!is_target_accession("MS:1000514"));
    }

    #[test]
    fn extract_targets_from_cv_params_removes_target_params_from_input() {
        let mut params = vec![
            make_cv_param("MS:1000511"),
            make_cv_param(&format!("MS:{ACC_SUFFIX_TARGET_MZ}")),
            make_cv_param("MS:1000515"),
            make_cv_param(&format!("MS:{ACC_SUFFIX_TARGET_NAME}")),
        ];

        let targets = extract_targets_from_cv_params(&mut params);

        assert_eq!(params.len(), 2);
        assert_eq!(params[0].accession.as_deref(), Some("MS:1000511"));
        assert_eq!(params[1].accession.as_deref(), Some("MS:1000515"));
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].cv_params.len(), 2);
    }

    #[test]
    fn extract_targets_from_cv_params_groups_by_target_mz_boundary() {
        let mut params = vec![
            make_cv_param(&format!("MS:{ACC_SUFFIX_TARGET_MZ}")),
            make_cv_param(&format!("MS:{ACC_SUFFIX_TARGET_NAME}")),
            make_cv_param(&format!("MS:{ACC_SUFFIX_TARGET_MZ}")),
            make_cv_param(&format!("MS:{ACC_SUFFIX_TARGET_END_MZ}")),
        ];

        let targets = extract_targets_from_cv_params(&mut params);

        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].cv_params.len(), 2);
        assert_eq!(targets[1].cv_params.len(), 2);
        assert!(params.is_empty());
    }

    #[test]
    fn extract_targets_from_cv_params_returns_empty_when_no_target_params_present() {
        let mut params = vec![make_cv_param("MS:1000511"), make_cv_param("MS:1000515")];
        let original_len = params.len();
        let targets = extract_targets_from_cv_params(&mut params);

        assert!(targets.is_empty());
        assert_eq!(params.len(), original_len);
    }

    #[test]
    fn extract_targets_from_cv_params_handles_empty_input() {
        let mut params: Vec<CvParam> = Vec::new();
        let targets = extract_targets_from_cv_params(&mut params);
        assert!(targets.is_empty());
    }

    #[test]
    fn extract_targets_from_cv_params_single_target_no_mz_boundary() {
        let mut params = vec![make_cv_param(&format!("MS:{ACC_SUFFIX_TARGET_NAME}"))];
        let targets = extract_targets_from_cv_params(&mut params);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].cv_params.len(), 1);
    }

    #[test]
    fn make_target_produces_target_with_empty_refs_and_user_params() {
        let cv_params = vec![make_cv_param("MS:1000827")];
        let target = make_target(cv_params);
        assert_eq!(target.cv_params.len(), 1);
        assert!(target.referenceable_param_group_refs.is_empty());
        assert!(target.user_params.is_empty());
    }
}
