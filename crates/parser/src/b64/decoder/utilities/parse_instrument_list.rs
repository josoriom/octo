use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decoder::decode::Metadatum,
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
pub fn parse_instrument_list<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<InstrumentList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
    }

    let list_id = children_lookup
        .all_ids(TagId::InstrumentConfigurationList)
        .first()
        .copied();

    let instrument_ids: &[u32] = list_id
        .map(|id| children_lookup.ids_for(id, TagId::Instrument))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.all_ids(TagId::Instrument));

    if instrument_ids.is_empty() {
        return None;
    }

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let instrument = instrument_ids
        .iter()
        .map(|&instrument_id| {
            parse_instrument(
                children_lookup,
                &owner_rows,
                instrument_id,
                policy,
                &mut param_buffer,
            )
        })
        .collect::<Vec<_>>();

    Some(InstrumentList {
        count: Some(instrument.len()),
        instrument,
    })
}

#[inline]
pub fn parse_data_processing_list<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<DataProcessingList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
    }

    let list_id = children_lookup
        .all_ids(TagId::DataProcessingList)
        .first()
        .copied();

    let data_processing_ids: &[u32] = list_id
        .map(|id| children_lookup.ids_for(id, TagId::DataProcessing))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.all_ids(TagId::DataProcessing));

    if data_processing_ids.is_empty() {
        return None;
    }

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let data_processing = data_processing_ids
        .iter()
        .map(|&data_processing_id| {
            parse_data_processing(
                children_lookup,
                &owner_rows,
                data_processing_id,
                policy,
                &mut param_buffer,
            )
        })
        .collect::<Vec<_>>();

    Some(DataProcessingList {
        count: Some(data_processing.len()),
        data_processing,
    })
}

#[inline]
fn parse_instrument<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    instrument_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Instrument {
    let rows = owner_rows.get(instrument_id);

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, instrument_id, policy, param_buffer);
    let (cv_param, user_param) = parse_cv_and_user_params(param_buffer);

    Instrument {
        id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
        scan_settings_ref: get_attr_text(rows, ACC_ATTR_SCAN_SETTINGS_REF)
            .filter(|s| !s.is_empty())
            .map(|s| ScanSettingsRef { r#ref: s }),
        cv_param,
        user_param,
        referenceable_param_group_ref: parse_param_group_refs(
            children_lookup,
            owner_rows,
            instrument_id,
        ),
        component_list: parse_component_list(
            children_lookup,
            owner_rows,
            instrument_id,
            policy,
            param_buffer,
        ),
        software_ref: parse_software_ref(children_lookup, owner_rows, instrument_id),
    }
}

#[inline]
fn parse_data_processing<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    data_processing_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> DataProcessing {
    let rows = owner_rows.get(data_processing_id);

    let methods = children_lookup
        .ids_for(data_processing_id, TagId::ProcessingMethod)
        .iter()
        .map(|&method_id| {
            let method_rows = owner_rows.get(method_id);

            param_buffer.clear();
            children_lookup.get_param_rows_into(owner_rows, method_id, policy, param_buffer);
            let (cv_param, user_param) = parse_cv_and_user_params(param_buffer);

            ProcessingMethod {
                order: get_attr_text(method_rows, ACC_ATTR_ORDER).and_then(|s| s.parse().ok()),
                software_ref: get_attr_text(method_rows, ACC_ATTR_SOFTWARE_REF),
                referenceable_param_group_ref: parse_param_group_refs(
                    children_lookup,
                    owner_rows,
                    method_id,
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
fn parse_component_list<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    instrument_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Option<ComponentList> {
    let component_root_id = children_lookup
        .ids_for(instrument_id, TagId::ComponentList)
        .first()
        .copied()
        .unwrap_or(instrument_id);

    let source_ids = children_lookup.ids_for(component_root_id, TagId::ComponentSource);
    let analyzer_ids = children_lookup.ids_for(component_root_id, TagId::ComponentAnalyzer);
    let detector_ids = children_lookup.ids_for(component_root_id, TagId::ComponentDetector);

    if source_ids.is_empty() && analyzer_ids.is_empty() && detector_ids.is_empty() {
        return None;
    }

    let source: Vec<Source> = source_ids
        .iter()
        .map(|&source_id| {
            parse_source(children_lookup, owner_rows, source_id, policy, param_buffer)
        })
        .collect();

    let analyzer: Vec<Analyzer> = analyzer_ids
        .iter()
        .map(|&analyzer_id| {
            parse_analyzer(
                children_lookup,
                owner_rows,
                analyzer_id,
                policy,
                param_buffer,
            )
        })
        .collect();

    let detector: Vec<Detector> = detector_ids
        .iter()
        .map(|&detector_id| {
            parse_detector(
                children_lookup,
                owner_rows,
                detector_id,
                policy,
                param_buffer,
            )
        })
        .collect();

    Some(ComponentList {
        count: Some(source.len() + analyzer.len() + detector.len()),
        source,
        analyzer,
        detector,
    })
}

#[inline]
fn parse_source<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    source_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Source {
    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, source_id, policy, param_buffer);
    let (cv_param, user_param) = parse_cv_and_user_params(param_buffer);

    Source {
        order: get_attr_text(owner_rows.get(source_id), ACC_ATTR_ORDER)
            .and_then(|s| s.parse().ok()),
        referenceable_param_group_ref: parse_param_group_refs(
            children_lookup,
            owner_rows,
            source_id,
        ),
        cv_param,
        user_param,
    }
}

#[inline]
fn parse_analyzer<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    analyzer_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Analyzer {
    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, analyzer_id, policy, param_buffer);
    let (cv_param, user_param) = parse_cv_and_user_params(param_buffer);

    Analyzer {
        order: get_attr_text(owner_rows.get(analyzer_id), ACC_ATTR_ORDER)
            .and_then(|s| s.parse().ok()),
        referenceable_param_group_ref: parse_param_group_refs(
            children_lookup,
            owner_rows,
            analyzer_id,
        ),
        cv_param,
        user_param,
    }
}

#[inline]
fn parse_detector<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    detector_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Detector {
    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, detector_id, policy, param_buffer);
    let (cv_param, user_param) = parse_cv_and_user_params(param_buffer);

    Detector {
        order: get_attr_text(owner_rows.get(detector_id), ACC_ATTR_ORDER)
            .and_then(|s| s.parse().ok()),
        referenceable_param_group_ref: parse_param_group_refs(
            children_lookup,
            owner_rows,
            detector_id,
        ),
        cv_param,
        user_param,
    }
}

#[inline]
fn parse_software_ref(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    instrument_id: u32,
) -> Option<InstrumentSoftwareRef> {
    children_lookup
        .ids_for(instrument_id, TagId::SoftwareRef)
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
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    children_lookup
        .ids_for(parent_id, TagId::ReferenceableParamGroupRef)
        .iter()
        .filter_map(|&ref_id| {
            get_attr_text(owner_rows.get(ref_id), ACC_ATTR_REF)
                .map(|r| ReferenceableParamGroupRef { r#ref: r })
        })
        .collect()
}
