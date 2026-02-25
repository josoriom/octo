use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{
        ParamCollector, ParseError, attr, parse_chromatogram_list, parse_source_file_ref_list,
        parse_spectrum_list, parsing_workspace::ParsingWorkspace, read_cv_param,
        read_ref_group_ref, read_user_param,
    },
};

pub(crate) fn parse_run<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Run, ParseError> {
    let mut run = Run {
        id: attr(start, b"id").unwrap_or_default(),
        start_time_stamp: attr(start, b"startTimeStamp"),
        default_instrument_configuration_ref: attr(start, b"defaultInstrumentConfigurationRef")
            .or_else(|| attr(start, b"instrumentRef")),
        default_source_file_ref: attr(start, b"defaultSourceFileRef"),
        sample_ref: attr(start, b"sampleRef"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        match tag {
            TagId::CvParam => {
                run.receive_cv(read_cv_param(&element));
                Ok(true)
            }
            TagId::UserParam => {
                run.receive_user(read_user_param(&element));
                Ok(true)
            }
            TagId::ReferenceableParamGroupRef => {
                run.receive_ref_group(read_ref_group_ref(&element));
                Ok(true)
            }
            TagId::SourceFileRefList if is_open => {
                run.source_file_ref_list = Some(parse_source_file_ref_list(ws, &element)?);
                Ok(true)
            }
            TagId::SpectrumList if is_open => {
                run.spectrum_list = Some(parse_spectrum_list(ws, &element)?);
                Ok(true)
            }
            TagId::ChromatogramList if is_open => {
                run.chromatogram_list = Some(parse_chromatogram_list(ws, &element)?);
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;
    Ok(run)
}
