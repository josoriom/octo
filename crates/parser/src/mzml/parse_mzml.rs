use quick_xml::Reader;
use quick_xml::events::Event;
use std::io::Cursor;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{
        ParseError, attr_u32, attr_usize, drain_until_close, parse_cv_list,
        parse_data_processing_list, parse_file_description, parse_index_list,
        parse_instrument_list, parse_ref_param_group_list, parse_run, parse_sample_list,
        parse_scan_settings_list, parse_software_list, parsing_workspace::ParsingWorkspace,
        tag_id_from_bytes,
    },
};

pub fn parse_mzml(bytes: &[u8]) -> Result<MzML, ParseError> {
    let mut ws = ParsingWorkspace::new(Reader::from_reader(Cursor::new(bytes)));
    let mut mzml = MzML::default();
    let mut inside_mzml = false;

    loop {
        let event = ws.next_event()?;
        match event {
            Event::Start(e) => {
                let tid = tag_id_from_bytes(e.name().as_ref());
                if !inside_mzml {
                    if tid == TagId::MzML {
                        inside_mzml = true;
                    }
                    continue;
                }
                match tid {
                    TagId::CvList => mzml.cv_list = Some(parse_cv_list(&mut ws, &e)?),
                    TagId::FileDescription => {
                        mzml.file_description = Some(parse_file_description(&mut ws, &e)?)
                    }
                    TagId::ReferenceableParamGroupList => {
                        mzml.referenceable_param_group_list =
                            Some(parse_ref_param_group_list(&mut ws, &e)?)
                    }
                    TagId::SampleList => mzml.sample_list = Some(parse_sample_list(&mut ws, &e)?),
                    TagId::InstrumentConfigurationList => {
                        mzml.instrument_list = parse_instrument_list(&mut ws, &e)?
                    }
                    TagId::SoftwareList => {
                        mzml.software_list = Some(parse_software_list(&mut ws, &e)?)
                    }
                    TagId::DataProcessingList => {
                        mzml.data_processing_list = Some(parse_data_processing_list(&mut ws, &e)?)
                    }
                    TagId::ScanSettingsList | TagId::AcquisitionSettingsList => {
                        mzml.scan_settings_list = parse_scan_settings_list(&mut ws, &e)?
                    }
                    TagId::Run => mzml.run = parse_run(&mut ws, &e)?,
                    _ => drain_until_close(&mut ws, e.name().as_ref())?,
                }
            }
            Event::Empty(e) => {
                if !inside_mzml {
                    continue;
                }
                match tag_id_from_bytes(e.name().as_ref()) {
                    TagId::ReferenceableParamGroupList => {
                        mzml.referenceable_param_group_list = Some(ReferenceableParamGroupList {
                            count: attr_usize(&e, b"count"),
                            ..Default::default()
                        })
                    }
                    TagId::SampleList => {
                        mzml.sample_list = Some(SampleList {
                            count: attr_u32(&e, b"count"),
                            ..Default::default()
                        })
                    }
                    TagId::InstrumentConfigurationList => {
                        mzml.instrument_list = Some(InstrumentList {
                            count: attr_usize(&e, b"count"),
                            ..Default::default()
                        })
                    }
                    TagId::SoftwareList => {
                        mzml.software_list = Some(SoftwareList {
                            count: attr_usize(&e, b"count"),
                            ..Default::default()
                        })
                    }
                    TagId::DataProcessingList => {
                        mzml.data_processing_list = Some(DataProcessingList {
                            count: attr_usize(&e, b"count"),
                            ..Default::default()
                        })
                    }
                    TagId::ScanSettingsList | TagId::AcquisitionSettingsList => {
                        mzml.scan_settings_list = Some(ScanSettingsList {
                            count: attr_usize(&e, b"count"),
                            ..Default::default()
                        })
                    }
                    _ => {}
                }
            }
            Event::End(e) if tag_id_from_bytes(e.name().as_ref()) == TagId::MzML => break Ok(mzml),
            Event::Eof => break Ok(mzml),
            _ => {}
        }
    }
}

pub fn parse_indexed_mzml(bytes: &[u8]) -> Result<IndexedmzML, ParseError> {
    let mzml = parse_mzml(bytes)?;
    let index_list = parse_index_list(bytes)?.unwrap_or(IndexList {
        spectrum: Vec::new(),
        chromatogram: Vec::new(),
        index_list_offset: None,
        file_checksum: None,
    });

    Ok(IndexedmzML {
        index_list_offset: index_list.index_list_offset,
        file_checksum: index_list.file_checksum.clone(),
        mzml,
        index_list,
    })
}
