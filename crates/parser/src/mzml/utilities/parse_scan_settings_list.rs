use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{
        ParamCollector, ParseError, attr, attr_usize, parse_source_file_ref_list,
        parse_target_list, parsing_workspace::ParsingWorkspace, read_cv_param, read_ref_group_ref,
        read_user_param,
    },
};

pub(crate) fn parse_scan_settings_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Option<ScanSettingsList>, ParseError> {
    let mut list = ScanSettingsList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };

    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::ScanSettings && tag != TagId::AcquisitionSettings {
            return Ok(false);
        }
        if is_open {
            list.scan_settings.push(parse_scan_settings(ws, &element)?);
        } else {
            list.scan_settings.push(ScanSettings {
                id: attr(&element, b"id"),
                instrument_configuration_ref: attr(&element, b"instrumentConfigurationRef"),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(if list.scan_settings.is_empty() && list.count.is_none() {
        None
    } else {
        Some(list)
    })
}

fn parse_scan_settings<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ScanSettings, ParseError> {
    let mut settings = ScanSettings {
        id: attr(start, b"id"),
        instrument_configuration_ref: attr(start, b"instrumentConfigurationRef"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        match tag {
            TagId::CvParam => {
                settings.receive_cv(read_cv_param(&element));
                Ok(true)
            }
            TagId::UserParam => {
                settings.receive_user(read_user_param(&element));
                Ok(true)
            }
            TagId::ReferenceableParamGroupRef => {
                settings.receive_ref_group(read_ref_group_ref(&element));
                Ok(true)
            }
            TagId::SourceFileRefList if is_open => {
                settings.source_file_ref_list = Some(parse_source_file_ref_list(ws, &element)?);
                Ok(true)
            }
            TagId::TargetList if is_open => {
                settings.target_list = Some(parse_target_list(ws, &element)?);
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;
    Ok(settings)
}
