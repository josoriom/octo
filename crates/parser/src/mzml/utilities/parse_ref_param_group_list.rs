use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{ParseError, attr, attr_usize, parsing_workspace::ParsingWorkspace},
};

pub(crate) fn parse_ref_param_group_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ReferenceableParamGroupList, ParseError> {
    let mut list = ReferenceableParamGroupList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::ReferenceableParamGroup {
            return Ok(false);
        }
        if is_open {
            list.referenceable_param_groups
                .push(parse_ref_param_group(ws, &element)?);
        } else {
            list.referenceable_param_groups
                .push(ReferenceableParamGroup {
                    id: attr(&element, b"id").unwrap_or_default(),
                    ..Default::default()
                });
        }
        Ok(true)
    })?;
    Ok(list)
}

fn parse_ref_param_group<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ReferenceableParamGroup, ParseError> {
    let mut group = ReferenceableParamGroup {
        id: attr(start, b"id").unwrap_or_default(),
        ..Default::default()
    };
    ws.collect_params_into(start, &mut group)?;
    Ok(group)
}
