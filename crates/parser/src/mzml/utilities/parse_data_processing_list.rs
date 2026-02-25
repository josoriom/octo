use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{ParseError, attr, attr_u32, attr_usize, parsing_workspace::ParsingWorkspace},
};

pub(crate) fn parse_data_processing_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<DataProcessingList, ParseError> {
    let mut list = DataProcessingList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::DataProcessing {
            return Ok(false);
        }
        if is_open {
            list.data_processing
                .push(parse_data_processing(ws, &element)?);
        } else {
            list.data_processing.push(DataProcessing {
                id: attr(&element, b"id").unwrap_or_default(),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(list)
}

fn parse_data_processing<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<DataProcessing, ParseError> {
    let mut dp = DataProcessing {
        id: attr(start, b"id").unwrap_or_default(),
        software_ref: attr(start, b"softwareRef"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::ProcessingMethod {
            return Ok(false);
        }
        if is_open {
            dp.processing_method
                .push(parse_processing_method(ws, &element)?);
        } else {
            dp.processing_method.push(ProcessingMethod {
                order: attr_u32(&element, b"order"),
                software_ref: attr(&element, b"softwareRef"),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(dp)
}

fn parse_processing_method<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ProcessingMethod, ParseError> {
    let mut pm = ProcessingMethod {
        order: attr_u32(start, b"order"),
        software_ref: attr(start, b"softwareRef"),
        ..Default::default()
    };
    ws.collect_params_into(start, &mut pm)?;
    Ok(pm)
}
