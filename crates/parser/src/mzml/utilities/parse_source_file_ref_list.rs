use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{
        ParseError, attr_usize, parsing_workspace::ParsingWorkspace, read_source_file_ref,
    },
};

pub(crate) fn parse_source_file_ref_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<SourceFileRefList, ParseError> {
    let mut list = SourceFileRefList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |_ws, event| {
        let (tag, element, _) = event.into_parts();
        if tag == TagId::SourceFileRef {
            list.source_file_refs.push(read_source_file_ref(&element));
            Ok(true)
        } else {
            Ok(false)
        }
    })?;
    Ok(list)
}
