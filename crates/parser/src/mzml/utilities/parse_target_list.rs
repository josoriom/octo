use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{ParseError, attr_usize, parsing_workspace::ParsingWorkspace},
};

pub(crate) fn parse_target_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<TargetList, ParseError> {
    let mut list = TargetList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::Target {
            return Ok(false);
        }
        if is_open {
            list.targets.push(parse_target(ws, &element)?);
        } else {
            list.targets.push(Target::default());
        }
        Ok(true)
    })?;
    Ok(list)
}

fn parse_target<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Target, ParseError> {
    let mut target = Target::default();
    ws.collect_params_into(start, &mut target)?;
    Ok(target)
}
