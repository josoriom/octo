use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use std::io::{BufRead, Cursor};

use crate::mzml::{
    structs::*,
    utilities::{
        IndexTag, ParseError, attr, attr_any, classify_index_tag,
        parsing_workspace::ParsingWorkspace, read_element_text,
    },
};

pub(crate) fn parse_index_list(bytes: &[u8]) -> Result<Option<IndexList>, ParseError> {
    let mut ws = ParsingWorkspace::new(Reader::from_reader(Cursor::new(bytes)));
    let mut out = IndexList {
        spectrum: Vec::new(),
        chromatogram: Vec::new(),
        index_list_offset: None,
        file_checksum: None,
    };
    let mut found_any = false;

    loop {
        let event = ws.next_event()?;
        match event {
            Event::Start(e) => match classify_index_tag(e.name().as_ref()) {
                IndexTag::IndexList => {
                    let (spectra, chromatograms) = parse_index_list_entries(&mut ws, &e)?;
                    out.spectrum = spectra;
                    out.chromatogram = chromatograms;
                    found_any = true;
                }
                IndexTag::IndexListOffset => {
                    let raw = e.name().as_ref().to_vec();
                    out.index_list_offset =
                        read_element_text(&mut ws, &raw)?.trim().parse::<u64>().ok();
                    found_any = true;
                }
                IndexTag::FileChecksum => {
                    let raw = e.name().as_ref().to_vec();
                    out.file_checksum = Some(read_element_text(&mut ws, &raw)?);
                    found_any = true;
                }
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(if found_any { Some(out) } else { None })
}

pub(crate) fn parse_index_list_entries<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<(Vec<IndexOffset>, Vec<IndexOffset>), ParseError> {
    let mut spectrum_offsets = Vec::new();
    let mut chromatogram_offsets = Vec::new();

    ws.for_each_child(start, |ws, event| {
        let (_, element, is_open) = event.into_parts();
        if classify_index_tag(element.name().as_ref()) != IndexTag::Index || !is_open {
            return Ok(false);
        }
        let (index_name, offsets) = parse_named_index(ws, &element)?;
        match index_name.as_str() {
            "spectrum" => spectrum_offsets = offsets,
            "chromatogram" => chromatogram_offsets = offsets,
            _ => {}
        }
        Ok(true)
    })?;

    Ok((spectrum_offsets, chromatogram_offsets))
}

fn parse_named_index<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<(String, Vec<IndexOffset>), ParseError> {
    let index_name = attr(start, b"name").unwrap_or_default();
    let mut offsets = Vec::new();

    ws.for_each_child(start, |ws, event| {
        let (_, element, is_open) = event.into_parts();
        match (classify_index_tag(element.name().as_ref()), is_open) {
            (IndexTag::Offset, true) => {
                offsets.push(parse_offset_element(ws, &element)?);
                Ok(true)
            }
            (IndexTag::Offset, false) => {
                offsets.push(IndexOffset {
                    id_ref: attr_any(&element, &[b"idRef", b"idref"]),
                    offset: 0,
                });
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;

    Ok((index_name, offsets))
}

fn parse_offset_element<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<IndexOffset, ParseError> {
    let id_ref = attr_any(start, &[b"idRef", b"idref"]);
    let closing = start.name().as_ref().to_vec();
    let text = read_element_text(ws, &closing)?;
    Ok(IndexOffset {
        id_ref,
        offset: text.trim().parse::<u64>().unwrap_or(0),
    })
}
