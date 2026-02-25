use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::utilities::parse_product_list::parse_product;
use crate::mzml::utilities::{
    attr, attr_u32, attr_usize, parse_bda_list, parse_precursor, read_cv_param, read_ref_group_ref,
    read_user_param,
};
use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{ParamCollector, ParseError, parsing_workspace::ParsingWorkspace},
};

pub(crate) fn parse_chromatogram_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ChromatogramList, ParseError> {
    let mut list = ChromatogramList {
        count: attr_usize(start, b"count"),
        default_data_processing_ref: attr(start, b"defaultDataProcessingRef"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::Chromatogram {
            return Ok(false);
        }
        if is_open {
            list.chromatograms.push(parse_chromatogram(ws, &element)?);
        } else {
            list.chromatograms.push(Chromatogram {
                id: attr(&element, b"id").unwrap_or_default(),
                index: attr_u32(&element, b"index"),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(list)
}

fn parse_chromatogram<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Chromatogram, ParseError> {
    let mut chrom = Chromatogram {
        id: attr(start, b"id").unwrap_or_default(),
        native_id: attr(start, b"nativeID"),
        index: attr_u32(start, b"index"),
        default_array_length: attr_usize(start, b"defaultArrayLength"),
        data_processing_ref: attr(start, b"dataProcessingRef"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        match tag {
            TagId::CvParam => {
                chrom.receive_cv(read_cv_param(&element));
                Ok(true)
            }
            TagId::UserParam => {
                chrom.receive_user(read_user_param(&element));
                Ok(true)
            }
            TagId::ReferenceableParamGroupRef => {
                chrom.receive_ref_group(read_ref_group_ref(&element));
                Ok(true)
            }
            TagId::Precursor if is_open => {
                chrom.precursor = Some(parse_precursor(ws, &element)?);
                Ok(true)
            }
            TagId::Product if is_open => {
                chrom.product = Some(parse_product(ws, &element)?);
                Ok(true)
            }
            TagId::BinaryDataArrayList if is_open => {
                chrom.binary_data_array_list = Some(parse_bda_list(ws, &element)?);
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;
    Ok(chrom)
}
