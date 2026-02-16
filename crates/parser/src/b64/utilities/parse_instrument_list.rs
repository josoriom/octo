use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{
            ACC_ATTR_ID, ACC_ATTR_ORDER, ACC_ATTR_REF, ACC_ATTR_SCAN_SETTINGS_REF,
            ACC_ATTR_SOFTWARE_REF,
        },
        schema::TagId,
        structs::{
            Analyzer, ComponentList, DataProcessing, DataProcessingList, Detector, Instrument,
            InstrumentList, InstrumentSoftwareRef, ProcessingMethod, ReferenceableParamGroupRef,
            ScanSettingsRef, Source,
        },
    },
};

#[inline]
pub fn parse_instrument_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<InstrumentList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let list_id = children_lookup
        .all_ids(TagId::InstrumentConfigurationList)
        .first()
        .copied();
    let ids = list_id
        .map(|id| children_lookup.ids_for(id, TagId::Instrument))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.all_ids(TagId::Instrument).to_vec());

    if ids.is_empty() {
        return None;
    }

    let instrument = ids
        .iter()
        .map(|&id| parse_instrument(children_lookup, &owner_rows, id))
        .collect::<Vec<_>>();

    Some(InstrumentList {
        count: Some(instrument.len()),
        instrument,
    })
}

#[inline]
pub fn parse_data_processing_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<DataProcessingList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let list_id = children_lookup
        .all_ids(TagId::DataProcessingList)
        .first()
        .copied();
    let ids = list_id
        .map(|id| children_lookup.ids_for(id, TagId::DataProcessing))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.all_ids(TagId::DataProcessing).to_vec());

    if ids.is_empty() {
        return None;
    }

    let data_processing = ids
        .iter()
        .map(|&id| parse_data_processing(children_lookup, &owner_rows, id))
        .collect::<Vec<_>>();

    Some(DataProcessingList {
        count: Some(data_processing.len()),
        data_processing,
    })
}

#[inline]
fn parse_instrument(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    id_u32: u32,
) -> Instrument {
    let rows = owner_rows.get(id_u32);
    let (cv_param, user_param) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id_u32));

    Instrument {
        id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
        scan_settings_ref: get_attr_text(rows, ACC_ATTR_SCAN_SETTINGS_REF)
            .filter(|s| !s.is_empty())
            .map(|s| ScanSettingsRef { r#ref: s }),
        cv_param,
        user_param,
        referenceable_param_group_ref: parse_param_group_refs(children_lookup, owner_rows, id_u32),
        component_list: parse_component_list(children_lookup, owner_rows, id_u32),
        software_ref: parse_software_ref(children_lookup, owner_rows, id_u32),
    }
}

#[inline]
fn parse_data_processing(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    id_u32: u32,
) -> DataProcessing {
    let rows = owner_rows.get(id_u32);
    let methods = children_lookup
        .ids_for(id_u32, TagId::ProcessingMethod)
        .iter()
        .map(|&m_id| {
            let m_rows = owner_rows.get(m_id);
            let (cv_param, user_param) =
                parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, m_id));
            ProcessingMethod {
                order: get_attr_text(m_rows, ACC_ATTR_ORDER).and_then(|s| s.parse().ok()),
                software_ref: get_attr_text(m_rows, ACC_ATTR_SOFTWARE_REF),
                referenceable_param_group_ref: parse_param_group_refs(
                    children_lookup,
                    owner_rows,
                    m_id,
                ),
                cv_param,
                user_param,
            }
        })
        .collect();

    DataProcessing {
        id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
        software_ref: get_attr_text(rows, ACC_ATTR_SOFTWARE_REF),
        processing_method: methods,
    }
}

#[inline]
fn parse_component_list(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    instrument_id: u32,
) -> Option<ComponentList> {
    let list_id = children_lookup
        .ids_for(instrument_id, TagId::ComponentList)
        .first()
        .copied();
    let root = list_id.unwrap_or(instrument_id);

    let source_ids = children_lookup.ids_for(root, TagId::ComponentSource);
    let analyzer_ids = children_lookup.ids_for(root, TagId::ComponentAnalyzer);
    let detector_ids = children_lookup.ids_for(root, TagId::ComponentDetector);

    if source_ids.is_empty() && analyzer_ids.is_empty() && detector_ids.is_empty() {
        return None;
    }

    let source: Vec<Source> = source_ids
        .iter()
        .map(|&id| parse_source(children_lookup, owner_rows, id))
        .collect();
    let analyzer: Vec<Analyzer> = analyzer_ids
        .iter()
        .map(|&id| parse_analyzer(children_lookup, owner_rows, id))
        .collect();
    let detector: Vec<Detector> = detector_ids
        .iter()
        .map(|&id| parse_detector(children_lookup, owner_rows, id))
        .collect();

    Some(ComponentList {
        count: Some(source.len() + analyzer.len() + detector.len()),
        source,
        analyzer,
        detector,
    })
}

#[inline]
fn parse_source(children_lookup: &ChildrenLookup, owner_rows: &OwnerRows, id: u32) -> Source {
    let (cv_param, user_param) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));
    Source {
        order: get_attr_text(owner_rows.get(id), ACC_ATTR_ORDER).and_then(|s| s.parse().ok()),
        referenceable_param_group_ref: parse_param_group_refs(children_lookup, owner_rows, id),
        cv_param,
        user_param,
    }
}

#[inline]
fn parse_analyzer(children_lookup: &ChildrenLookup, owner_rows: &OwnerRows, id: u32) -> Analyzer {
    let (cv_param, user_param) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));
    Analyzer {
        order: get_attr_text(owner_rows.get(id), ACC_ATTR_ORDER).and_then(|s| s.parse().ok()),
        referenceable_param_group_ref: parse_param_group_refs(children_lookup, owner_rows, id),
        cv_param,
        user_param,
    }
}

#[inline]
fn parse_detector(children_lookup: &ChildrenLookup, owner_rows: &OwnerRows, id: u32) -> Detector {
    let (cv_param, user_param) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));
    Detector {
        order: get_attr_text(owner_rows.get(id), ACC_ATTR_ORDER).and_then(|s| s.parse().ok()),
        referenceable_param_group_ref: parse_param_group_refs(children_lookup, owner_rows, id),
        cv_param,
        user_param,
    }
}

#[inline]
fn parse_software_ref(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    id: u32,
) -> Option<InstrumentSoftwareRef> {
    children_lookup
        .ids_for(id, TagId::SoftwareRef)
        .first()
        .and_then(|&ref_id| {
            get_attr_text(owner_rows.get(ref_id), ACC_ATTR_REF)
                .map(|r| InstrumentSoftwareRef { r#ref: r })
        })
}

#[inline]
fn parse_param_group_refs(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    children_lookup
        .ids_for(id, TagId::ReferenceableParamGroupRef)
        .iter()
        .filter_map(|&ref_id| {
            get_attr_text(owner_rows.get(ref_id), ACC_ATTR_REF)
                .map(|r| ReferenceableParamGroupRef { r#ref: r })
        })
        .collect()
}
