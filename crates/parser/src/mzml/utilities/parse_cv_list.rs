use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{ParseError, attr_usize, parsing_workspace::ParsingWorkspace, read_cv_entry},
};

pub(crate) fn parse_cv_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<CvList, ParseError> {
    let mut list = CvList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |_ws, event| {
        let (tag, element, _) = event.into_parts();
        if tag == TagId::Cv {
            list.cv.push(read_cv_entry(&element));
            Ok(true)
        } else {
            Ok(false)
        }
    })?;
    Ok(list)
}
