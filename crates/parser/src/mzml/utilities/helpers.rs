use std::{io::BufRead, str::from_utf8};

use quick_xml::events::{BytesStart, Event};

use crate::{
    CvEntry, CvParam, ReferenceableParamGroupRef, SoftwareParam, SourceFileRef, UserParam,
    mzml::{
        schema::TagId,
        utilities::{ParseError, ParsingWorkspace, normalize_tag},
    },
};

#[inline]
pub fn xml_local_name(mut raw: &[u8]) -> &[u8] {
    if raw.first() == Some(&b'{') {
        if let Some(end) = raw.iter().position(|&b| b == b'}') {
            raw = &raw[end + 1..];
        }
    }
    if let Some(colon) = raw.iter().rposition(|&b| b == b':') {
        &raw[colon + 1..]
    } else {
        raw
    }
}

#[inline]
pub fn tag_id_from_bytes(raw: &[u8]) -> TagId {
    let local = from_utf8(xml_local_name(raw)).unwrap_or("");
    TagId::from_xml_tag(normalize_tag(local))
}

pub fn drain_until_close<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    closing_bytes: &[u8],
) -> Result<(), ParseError> {
    let mut depth = 1usize;
    loop {
        match ws.next_event()? {
            Event::Start(_) => depth += 1,
            Event::End(e) => {
                depth -= 1;
                if depth == 0 && e.name().as_ref() == closing_bytes {
                    break Ok(());
                }
            }
            Event::Eof => break Ok(()),
            _ => {}
        }
    }
}

pub fn read_element_text<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    closing_bytes: &[u8],
) -> Result<String, ParseError> {
    let mut text = String::new();
    loop {
        match ws.next_event()? {
            Event::Text(t) => text.push_str(&t.decode().map_err(quick_xml::Error::from)?),
            Event::CData(t) => text.push_str(&String::from_utf8_lossy(&t.into_inner())),
            Event::End(e) if e.name().as_ref() == closing_bytes => break,
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(text)
}

pub fn read_base64_binary<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    closing_bytes: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), ParseError> {
    out.clear();
    loop {
        match ws.next_event()? {
            Event::Text(t) => out.extend(
                t.as_ref()
                    .iter()
                    .copied()
                    .filter(|b| !b.is_ascii_whitespace()),
            ),
            Event::CData(t) => out.extend(
                t.into_inner()
                    .iter()
                    .copied()
                    .filter(|b| !b.is_ascii_whitespace()),
            ),
            Event::End(e) if e.name().as_ref() == closing_bytes => break,
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(())
}

pub fn attr_any(element: &BytesStart, candidate_names: &[&[u8]]) -> Option<String> {
    for a in element.attributes().with_checks(false).flatten() {
        if candidate_names.iter().any(|n| *n == a.key.as_ref()) {
            return a.unescape_value().ok().map(|v| v.to_string());
        }
    }
    None
}

#[inline]
pub fn attr(e: &BytesStart, name: &[u8]) -> Option<String> {
    attr_any(e, &[name])
}
#[inline]
pub fn attr_u32(e: &BytesStart, name: &[u8]) -> Option<u32> {
    attr(e, name).and_then(|s| s.parse().ok())
}
#[inline]
pub fn attr_usize(e: &BytesStart, name: &[u8]) -> Option<usize> {
    attr(e, name).and_then(|s| s.parse().ok())
}

#[inline]
pub fn read_cv_param(e: &BytesStart) -> CvParam {
    CvParam {
        cv_ref: attr_any(e, &[b"cvRef", b"cvLabel"]),
        accession: attr(e, b"accession"),
        name: attr(e, b"name").unwrap_or_default(),
        value: attr(e, b"value"),
        unit_cv_ref: attr_any(e, &[b"unitCvRef", b"unitCvLabel"]),
        unit_name: attr(e, b"unitName"),
        unit_accession: attr(e, b"unitAccession"),
    }
}

#[inline]
pub fn read_user_param(e: &BytesStart) -> UserParam {
    UserParam {
        name: attr(e, b"name").unwrap_or_default(),
        r#type: attr(e, b"type"),
        unit_accession: attr(e, b"unitAccession"),
        unit_cv_ref: attr_any(e, &[b"unitCvRef", b"unitCvLabel"]),
        unit_name: attr(e, b"unitName"),
        value: attr(e, b"value"),
    }
}

#[inline]
pub fn read_software_param(e: &BytesStart) -> SoftwareParam {
    SoftwareParam {
        cv_ref: attr_any(e, &[b"cvRef", b"cvLabel"]),
        accession: attr(e, b"accession").unwrap_or_default(),
        name: attr(e, b"name").unwrap_or_default(),
        version: attr(e, b"version"),
    }
}

#[inline]
pub fn read_ref_group_ref(e: &BytesStart) -> ReferenceableParamGroupRef {
    ReferenceableParamGroupRef {
        r#ref: attr(e, b"ref").unwrap_or_default(),
    }
}
#[inline]
pub fn read_source_file_ref(e: &BytesStart) -> SourceFileRef {
    SourceFileRef {
        r#ref: attr(e, b"ref").unwrap_or_default(),
    }
}
#[inline]
pub fn read_cv_entry(e: &BytesStart) -> CvEntry {
    CvEntry {
        id: attr_any(e, &[b"id", b"cvLabel"]).unwrap_or_default(),
        full_name: attr(e, b"fullName"),
        version: attr(e, b"version"),
        uri: attr(e, b"URI"),
    }
}
