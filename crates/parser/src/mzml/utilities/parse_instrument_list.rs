use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{
        ParamCollector, ParseError, attr, attr_usize, parse_component_list,
        parsing_workspace::ParsingWorkspace, read_cv_param, read_ref_group_ref, read_user_param,
    },
};

pub(crate) fn parse_instrument_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Option<InstrumentList>, ParseError> {
    let mut list = InstrumentList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::Instrument {
            return Ok(false);
        }
        if is_open {
            list.instrument
                .push(parse_instrument_with_body(ws, &element)?);
        } else {
            list.instrument.push(parse_instrument_empty(&element));
        }
        Ok(true)
    })?;
    Ok(if list.instrument.is_empty() && list.count.is_none() {
        None
    } else {
        Some(list)
    })
}

fn parse_instrument_with_body<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Instrument, ParseError> {
    let mut instrument = Instrument {
        id: attr(start, b"id").unwrap_or_default(),
        scan_settings_ref: attr(start, b"scanSettingsRef").map(|r| ScanSettingsRef { r#ref: r }),
        software_ref: attr(start, b"softwareRef").map(|r| InstrumentSoftwareRef { r#ref: r }),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        match tag {
            TagId::CvParam => {
                instrument.receive_cv(read_cv_param(&element));
                Ok(true)
            }
            TagId::UserParam => {
                instrument.receive_user(read_user_param(&element));
                Ok(true)
            }
            TagId::ReferenceableParamGroupRef => {
                instrument.receive_ref_group(read_ref_group_ref(&element));
                Ok(true)
            }
            TagId::ComponentList if is_open => {
                instrument.component_list = Some(parse_component_list(ws, &element)?);
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;
    Ok(instrument)
}

fn parse_instrument_empty(start: &BytesStart<'_>) -> Instrument {
    Instrument {
        id: attr(start, b"id").unwrap_or_default(),
        scan_settings_ref: attr(start, b"scanSettingsRef").map(|r| ScanSettingsRef { r#ref: r }),
        software_ref: attr(start, b"softwareRef").map(|r| InstrumentSoftwareRef { r#ref: r }),
        ..Default::default()
    }
}
