use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{ParseError, attr_u32, attr_usize, parsing_workspace::ParsingWorkspace},
};

pub(crate) fn parse_component_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ComponentList, ParseError> {
    let mut list = ComponentList {
        count: attr_usize(start, b"count"),
        source: Vec::new(),
        analyzer: Vec::new(),
        detector: Vec::new(),
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, _) = event.into_parts();
        match tag {
            TagId::ComponentSource => {
                let mut source = Source {
                    order: attr_u32(&element, b"order"),
                    ..Default::default()
                };
                ws.collect_params_into(&element, &mut source)?;
                list.source.push(source);
            }
            TagId::ComponentAnalyzer => {
                let mut analyzer = Analyzer {
                    order: attr_u32(&element, b"order"),
                    ..Default::default()
                };
                ws.collect_params_into(&element, &mut analyzer)?;
                list.analyzer.push(analyzer);
            }
            TagId::ComponentDetector => {
                let mut detector = Detector {
                    order: attr_u32(&element, b"order"),
                    ..Default::default()
                };
                ws.collect_params_into(&element, &mut detector)?;
                list.detector.push(detector);
            }
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(list)
}
