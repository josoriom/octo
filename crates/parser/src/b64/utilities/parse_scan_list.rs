use crate::{
    ScanList, ScanWindow, ScanWindowList,
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::{get_attr_text, get_attr_u32},
        parse_cv_and_user_params,
    },
    mzml::{
        attr_meta::{
            ACC_ATTR_COUNT, ACC_ATTR_EXTERNAL_SPECTRUM_ID, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
            ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPECTRUM_REF,
        },
        schema::TagId,
        structs::Scan,
    },
};

#[inline]
pub fn parse_scan_list(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
) -> Option<ScanList> {
    let desc_id = children_lookup.first_id(spectrum_id, TagId::SpectrumDescription);
    let list_id = children_lookup
        .first_id(spectrum_id, TagId::ScanList)
        .or_else(|| desc_id.and_then(|id| children_lookup.first_id(id, TagId::ScanList)));

    let scan_ids = children_lookup.ids_for(list_id.or(desc_id).unwrap_or(spectrum_id), TagId::Scan);
    if scan_ids.is_empty() {
        return None;
    }

    let scans = scan_ids
        .iter()
        .map(|&id| {
            let rows = owner_rows.get(id);
            let param_rows = &children_lookup.get_param_rows(owner_rows, id);
            let (cv_params, user_params) = parse_cv_and_user_params(&param_rows);

            Scan {
                instrument_configuration_ref: get_attr_text(
                    rows,
                    ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
                ),
                spectrum_ref: get_attr_text(rows, ACC_ATTR_SPECTRUM_REF),
                source_file_ref: get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF),
                external_spectrum_id: get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID),
                scan_window_list: parse_scan_window_list(owner_rows, children_lookup, id),
                cv_params,
                user_params,
                referenceable_param_group_refs: Vec::new(),
            }
        })
        .collect::<Vec<_>>();

    let (cv_params, user_params) = list_id
        .map(|id| parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id)))
        .unwrap_or_default();

    Some(ScanList {
        count: Some(scans.len()),
        cv_params,
        user_params,
        scans,
    })
}

#[inline]
fn parse_scan_window_list(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    scan_id: u32,
) -> Option<ScanWindowList> {
    let list_id = children_lookup.first_id(scan_id, TagId::ScanWindowList);
    let window_ids = children_lookup.ids_for(list_id.unwrap_or(scan_id), TagId::ScanWindow);

    if window_ids.is_empty() {
        return None;
    }

    let scan_windows = window_ids
        .iter()
        .map(|&id| {
            let (cv_params, user_params) =
                parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));
            ScanWindow {
                cv_params,
                user_params,
            }
        })
        .collect::<Vec<_>>();

    let count = list_id
        .and_then(|id| get_attr_u32(owner_rows.get(id), ACC_ATTR_COUNT))
        .map(|v| v as usize)
        .or(Some(scan_windows.len()));

    Some(ScanWindowList {
        count,
        scan_windows,
    })
}
