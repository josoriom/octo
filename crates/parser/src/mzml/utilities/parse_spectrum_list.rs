use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{
        ParamCollector, ParseError, attr, attr_u32, attr_usize, parse_bda, parse_bda_list,
        parse_precursor, parse_precursor_list::parse_precursor_list,
        parse_product_list::parse_product_list, parse_scan, parse_scan_list,
        parsing_workspace::ParsingWorkspace, read_cv_param, read_ref_group_ref, read_user_param,
    },
};

pub(crate) fn parse_spectrum_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<SpectrumList, ParseError> {
    let mut list = SpectrumList {
        count: attr_usize(start, b"count"),
        default_data_processing_ref: attr(start, b"defaultDataProcessingRef"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::Spectrum {
            return Ok(false);
        }
        if is_open {
            list.spectra.push(parse_spectrum(ws, &element)?);
        } else {
            list.spectra.push(Spectrum {
                id: attr(&element, b"id").unwrap_or_default(),
                index: attr_u32(&element, b"index"),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(list)
}

fn parse_spectrum<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Spectrum, ParseError> {
    let mut spectrum = Spectrum {
        id: attr(start, b"id").unwrap_or_default(),
        index: attr_u32(start, b"index"),
        scan_number: attr_u32(start, b"scanNumber"),
        ms_level: attr_u32(start, b"msLevel"),
        default_array_length: attr_usize(start, b"defaultArrayLength"),
        native_id: attr(start, b"nativeID"),
        data_processing_ref: attr(start, b"dataProcessingRef"),
        source_file_ref: attr(start, b"sourceFileRef"),
        spot_id: attr(start, b"spotID"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        match tag {
            TagId::CvParam => {
                spectrum.receive_cv(read_cv_param(&element));
                Ok(true)
            }
            TagId::UserParam => {
                spectrum.receive_user(read_user_param(&element));
                Ok(true)
            }
            TagId::ReferenceableParamGroupRef => {
                spectrum.receive_ref_group(read_ref_group_ref(&element));
                Ok(true)
            }
            TagId::SpectrumDescription if is_open => {
                let result = parse_spectrum_description(ws, &element)?;
                spectrum.merge_description(result);
                Ok(true)
            }
            TagId::ScanList if is_open => {
                spectrum.scan_list = Some(parse_scan_list(ws, &element)?);
                Ok(true)
            }
            TagId::PrecursorList if is_open => {
                spectrum.precursor_list = Some(parse_precursor_list(ws, &element)?);
                Ok(true)
            }
            TagId::ProductList if is_open => {
                spectrum.product_list = Some(parse_product_list(ws, &element)?);
                Ok(true)
            }
            TagId::BinaryDataArrayList if is_open => {
                spectrum.binary_data_array_list = Some(parse_bda_list(ws, &element)?);
                Ok(true)
            }
            TagId::BinaryDataArray if is_open => {
                spectrum
                    .binary_data_array_list
                    .get_or_insert_with(Default::default)
                    .binary_data_arrays
                    .push(parse_bda(ws, &element)?);
                Ok(true)
            }
            TagId::Precursor if is_open => {
                push_precursor_onto_spectrum(ws, &element, &mut spectrum)?;
                Ok(true)
            }
            TagId::ScanList => {
                spectrum.scan_list = Some(ScanList {
                    count: attr_usize(&element, b"count"),
                    ..Default::default()
                });
                Ok(true)
            }
            TagId::PrecursorList => {
                spectrum.precursor_list = Some(PrecursorList {
                    count: attr_usize(&element, b"count"),
                    ..Default::default()
                });
                Ok(true)
            }
            TagId::ProductList => {
                spectrum.product_list = Some(ProductList {
                    count: attr_usize(&element, b"count"),
                    ..Default::default()
                });
                Ok(true)
            }
            TagId::BinaryDataArrayList => {
                spectrum.binary_data_array_list = Some(BinaryDataArrayList {
                    count: attr_usize(&element, b"count"),
                    ..Default::default()
                });
                Ok(true)
            }
            TagId::Precursor => {
                append_empty_precursor(&element, &mut spectrum);
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;
    Ok(spectrum)
}

fn parse_spectrum_description<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<SpectrumDescriptionResult, ParseError> {
    let mut result = SpectrumDescriptionResult {
        description: SpectrumDescription::default(),
        scan_list: None,
        precursor_list: None,
        ms_level_hint: None,
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        match tag {
            TagId::CvParam => {
                let cv = read_cv_param(&element);
                if result.ms_level_hint.is_none() && cv.accession.as_deref() == Some("MS:1000511") {
                    result.ms_level_hint = cv
                        .value
                        .as_deref()
                        .and_then(|v: &str| v.parse::<u32>().ok());
                }
                result.description.receive_cv(cv);
                Ok(true)
            }
            TagId::UserParam => {
                result.description.receive_user(read_user_param(&element));
                Ok(true)
            }
            TagId::ReferenceableParamGroupRef => {
                result
                    .description
                    .receive_ref_group(read_ref_group_ref(&element));
                Ok(true)
            }
            TagId::ScanList if is_open => {
                result.scan_list = Some(parse_scan_list(ws, &element)?);
                Ok(true)
            }
            TagId::PrecursorList if is_open => {
                result.precursor_list = Some(parse_precursor_list(ws, &element)?);
                Ok(true)
            }
            TagId::Scan if is_open => {
                let list = result.scan_list.get_or_insert_with(Default::default);
                list.scans.push(parse_scan(ws, &element)?);
                list.count = Some(list.scans.len());
                Ok(true)
            }
            TagId::Scan => {
                let list = result.scan_list.get_or_insert_with(Default::default);
                list.scans.push(Scan {
                    instrument_configuration_ref: attr(&element, b"instrumentConfigurationRef")
                        .or_else(|| attr(&element, b"instrumentRef")),
                    external_spectrum_id: attr(&element, b"externalSpectrumID"),
                    ..Default::default()
                });
                list.count = Some(list.scans.len());
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;
    Ok(result)
}

impl Spectrum {
    fn merge_description(&mut self, result: SpectrumDescriptionResult) {
        self.spectrum_description = Some(result.description);
        self.scan_list = self.scan_list.take().or(result.scan_list);
        self.precursor_list = self.precursor_list.take().or(result.precursor_list);
        if self.ms_level.is_none() {
            self.ms_level = result.ms_level_hint;
        }
    }
}

struct SpectrumDescriptionResult {
    description: SpectrumDescription,
    scan_list: Option<ScanList>,
    precursor_list: Option<PrecursorList>,
    ms_level_hint: Option<u32>,
}

fn push_precursor_onto_spectrum<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    element: &BytesStart<'_>,
    spectrum: &mut Spectrum,
) -> Result<(), ParseError> {
    let list = spectrum.precursor_list.get_or_insert_with(Default::default);
    list.precursors.push(parse_precursor(ws, element)?);
    list.count = Some(list.precursors.len());
    Ok(())
}

fn append_empty_precursor(element: &BytesStart<'_>, spectrum: &mut Spectrum) {
    let list = spectrum.precursor_list.get_or_insert_with(Default::default);
    list.precursors.push(Precursor {
        spectrum_ref: attr(element, b"spectrumRef"),
        source_file_ref: attr(element, b"sourceFileRef"),
        external_spectrum_id: attr(element, b"externalSpectrumID"),
        ..Default::default()
    });
    list.count = Some(list.precursors.len());
}
