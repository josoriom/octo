use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{ParseError, attr, attr_usize, parsing_workspace::ParsingWorkspace},
};

pub(crate) fn parse_software_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<SoftwareList, ParseError> {
    let mut list = SoftwareList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::Software {
            return Ok(false);
        }
        if is_open {
            list.software.push(parse_software(ws, &element)?);
        } else {
            list.software.push(Software {
                id: attr(&element, b"id").unwrap_or_default(),
                version: attr(&element, b"version"),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(list)
}

fn parse_software<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Software, ParseError> {
    let mut software = Software {
        id: attr(start, b"id").unwrap_or_default(),
        version: attr(start, b"version"),
        ..Default::default()
    };
    ws.collect_params_into(start, &mut software)?;
    Ok(software)
}
