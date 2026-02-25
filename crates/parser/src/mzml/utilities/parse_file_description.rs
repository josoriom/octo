use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{ParseError, attr, attr_any, attr_usize, parsing_workspace::ParsingWorkspace},
};

pub(crate) fn parse_file_description<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<FileDescription, ParseError> {
    let mut fd = FileDescription::default();
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        match (tag, is_open) {
            (TagId::FileContent, true) => {
                fd.file_content = parse_file_content(ws, &element)?;
                Ok(true)
            }
            (TagId::SourceFileList, true) => {
                fd.source_file_list = parse_source_file_list(ws, &element)?;
                Ok(true)
            }
            (TagId::Contact, true) => {
                fd.contacts.push(parse_contact(ws, &element)?);
                Ok(true)
            }
            (TagId::Contact, false) => {
                fd.contacts.push(Contact::default());
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;
    Ok(fd)
}

fn parse_file_content<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<FileContent, ParseError> {
    let mut fc = FileContent::default();
    ws.collect_params_into(start, &mut fc)?;
    Ok(fc)
}

fn parse_source_file_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<SourceFileList, ParseError> {
    let mut list = SourceFileList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::SourceFile {
            return Ok(false);
        }
        if is_open {
            list.source_file.push(parse_source_file(ws, &element)?);
        } else {
            list.source_file.push(SourceFile {
                id: attr(&element, b"id").unwrap_or_default(),
                name: attr_any(&element, &[b"name", b"sourceFileName"]).unwrap_or_default(),
                location: attr_any(&element, &[b"location", b"sourceFileLocation"])
                    .unwrap_or_default(),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(list)
}

fn parse_source_file<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<SourceFile, ParseError> {
    let mut sf = SourceFile {
        id: attr(start, b"id").unwrap_or_default(),
        name: attr_any(start, &[b"name", b"sourceFileName"]).unwrap_or_default(),
        location: attr_any(start, &[b"location", b"sourceFileLocation"]).unwrap_or_default(),
        ..Default::default()
    };
    ws.collect_params_into(start, &mut sf)?;
    Ok(sf)
}

fn parse_contact<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Contact, ParseError> {
    let mut contact = Contact::default();
    ws.collect_params_into(start, &mut contact)?;
    Ok(contact)
}
