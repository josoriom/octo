use crate::{
    ScanList, ScanWindow, ScanWindowList,
    b64::{
        attr_meta::{
            ACC_ATTR_COUNT, ACC_ATTR_EXTERNAL_SPECTRUM_ID, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
            ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPECTRUM_REF,
        },
        utilities::{
            children_lookup::{ChildrenLookup, DefaultMetadataPolicy, OwnerRows},
            common::{get_attr_text, get_attr_u32},
            parse_cv_and_user_params,
        },
    },
    decoder::decode::Metadatum,
    mzml::{schema::TagId, structs::Scan},
};

#[inline]
pub(crate) fn parse_scan_list<'a>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
) -> Option<ScanList> {
    let description_id = children_lookup
        .ids_for(spectrum_id, TagId::SpectrumDescription)
        .first()
        .copied();

    let scan_list_id = children_lookup
        .ids_for(spectrum_id, TagId::ScanList)
        .first()
        .copied()
        .or_else(|| {
            description_id.and_then(|id| {
                children_lookup
                    .ids_for(id, TagId::ScanList)
                    .first()
                    .copied()
            })
        });

    let scan_container_id = scan_list_id.or(description_id).unwrap_or(spectrum_id);
    let scan_ids = children_lookup.ids_for(scan_container_id, TagId::Scan);
    if scan_ids.is_empty() {
        return None;
    }

    let mut param_buffer: Vec<&Metadatum> = Vec::new();
    let policy = DefaultMetadataPolicy;

    let scans = scan_ids
        .iter()
        .map(|&scan_id| {
            let rows = owner_rows.get(scan_id);

            param_buffer.clear();
            children_lookup.get_param_rows_into(owner_rows, scan_id, &policy, &mut param_buffer);
            let (cv_params, user_params) = parse_cv_and_user_params(&param_buffer);

            Scan {
                instrument_configuration_ref: get_attr_text(
                    rows,
                    ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
                ),
                spectrum_ref: get_attr_text(rows, ACC_ATTR_SPECTRUM_REF),
                source_file_ref: get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF),
                external_spectrum_id: get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID),
                scan_window_list: parse_scan_window_list(
                    owner_rows,
                    children_lookup,
                    scan_id,
                    &policy,
                    &mut param_buffer,
                ),
                cv_params,
                user_params,
                referenceable_param_group_refs: Vec::new(),
            }
        })
        .collect::<Vec<_>>();

    let (cv_params, user_params) = scan_list_id
        .map(|id| {
            param_buffer.clear();
            children_lookup.get_param_rows_into(owner_rows, id, &policy, &mut param_buffer);
            parse_cv_and_user_params(&param_buffer)
        })
        .unwrap_or_default();

    Some(ScanList {
        count: Some(scans.len()),
        cv_params,
        user_params,
        scans,
    })
}

#[inline]
fn parse_scan_window_list<'a>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    scan_id: u32,
    policy: &DefaultMetadataPolicy,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Option<ScanWindowList> {
    let scan_window_list_id = children_lookup
        .ids_for(scan_id, TagId::ScanWindowList)
        .first()
        .copied();

    let window_ids =
        children_lookup.ids_for(scan_window_list_id.unwrap_or(scan_id), TagId::ScanWindow);

    if window_ids.is_empty() {
        return None;
    }

    let scan_windows = window_ids
        .iter()
        .map(|&window_id| {
            param_buffer.clear();
            children_lookup.get_param_rows_into(owner_rows, window_id, policy, param_buffer);
            let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);

            ScanWindow {
                cv_params,
                user_params,
            }
        })
        .collect::<Vec<_>>();

    let count = scan_window_list_id
        .and_then(|id| get_attr_u32(owner_rows.get(id), ACC_ATTR_COUNT))
        .map(|v| v as usize)
        .or(Some(scan_windows.len()));

    Some(ScanWindowList {
        count,
        scan_windows,
    })
}
