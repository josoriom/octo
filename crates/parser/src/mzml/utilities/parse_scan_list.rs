use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{
        ParamCollector, ParseError, attr, attr_usize, parsing_workspace::ParsingWorkspace,
        read_cv_param, read_ref_group_ref, read_user_param,
    },
};

pub(crate) fn parse_scan_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ScanList, ParseError> {
    let mut list = ScanList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::Scan {
            return Ok(false);
        }
        if is_open {
            list.scans.push(parse_scan(ws, &element)?);
        } else {
            list.scans.push(Scan {
                instrument_configuration_ref: attr(&element, b"instrumentConfigurationRef")
                    .or_else(|| attr(&element, b"instrumentRef")),
                external_spectrum_id: attr(&element, b"externalSpectrumID"),
                source_file_ref: attr(&element, b"sourceFileRef"),
                spectrum_ref: attr(&element, b"spectrumRef"),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(list)
}

pub(crate) fn parse_scan<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Scan, ParseError> {
    let mut scan = Scan {
        instrument_configuration_ref: attr(start, b"instrumentConfigurationRef")
            .or_else(|| attr(start, b"instrumentRef")),
        external_spectrum_id: attr(start, b"externalSpectrumID"),
        source_file_ref: attr(start, b"sourceFileRef"),
        spectrum_ref: attr(start, b"spectrumRef"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        match tag {
            TagId::CvParam => {
                scan.receive_cv(read_cv_param(&element));
                Ok(true)
            }
            TagId::UserParam => {
                scan.receive_user(read_user_param(&element));
                Ok(true)
            }
            TagId::ReferenceableParamGroupRef => {
                scan.receive_ref_group(read_ref_group_ref(&element));
                Ok(true)
            }
            TagId::ScanWindowList if is_open => {
                scan.scan_window_list = Some(parse_scan_window_list(ws, &element)?);
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;
    Ok(scan)
}

fn parse_scan_window_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ScanWindowList, ParseError> {
    let mut list = ScanWindowList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::ScanWindow {
            return Ok(false);
        }
        if is_open {
            list.scan_windows.push(parse_scan_window(ws, &element)?);
        } else {
            list.scan_windows.push(ScanWindow::default());
        }
        Ok(true)
    })?;
    Ok(list)
}

fn parse_scan_window<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ScanWindow, ParseError> {
    let mut window = ScanWindow::default();
    ws.collect_params_into(start, &mut window)?;
    Ok(window)
}
