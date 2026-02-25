use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::utilities::{attr, attr_usize};
use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{ParseError, parsing_workspace::ParsingWorkspace},
};

pub(crate) fn parse_precursor_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<PrecursorList, ParseError> {
    let mut list = PrecursorList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::Precursor {
            return Ok(false);
        }
        if is_open {
            list.precursors.push(parse_precursor(ws, &element)?);
        } else {
            list.precursors.push(Precursor {
                spectrum_ref: attr(&element, b"spectrumRef"),
                source_file_ref: attr(&element, b"sourceFileRef"),
                external_spectrum_id: attr(&element, b"externalSpectrumID"),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(list)
}

pub(crate) fn parse_precursor<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Precursor, ParseError> {
    let mut precursor = Precursor {
        spectrum_ref: attr(start, b"spectrumRef"),
        source_file_ref: attr(start, b"sourceFileRef"),
        external_spectrum_id: attr(start, b"externalSpectrumID"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        match tag {
            TagId::IsolationWindow if is_open => {
                precursor.isolation_window = Some(parse_isolation_window(ws, &element)?);
                Ok(true)
            }
            TagId::SelectedIonList if is_open => {
                precursor.selected_ion_list = Some(parse_selected_ion_list(ws, &element)?);
                Ok(true)
            }
            TagId::Activation if is_open => {
                precursor.activation = Some(parse_activation(ws, &element)?);
                Ok(true)
            }
            TagId::SelectedIon if is_open => {
                push_selected_ion_onto_precursor(ws, &element, &mut precursor)?;
                Ok(true)
            }
            TagId::IsolationWindow => {
                precursor.isolation_window = Some(IsolationWindow::default());
                Ok(true)
            }
            TagId::SelectedIonList => {
                precursor.selected_ion_list = Some(SelectedIonList::default());
                Ok(true)
            }
            TagId::SelectedIon => {
                append_empty_selected_ion(&mut precursor);
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;
    Ok(precursor)
}

pub(crate) fn parse_isolation_window<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<IsolationWindow, ParseError> {
    let mut window = IsolationWindow::default();
    ws.collect_params_into(start, &mut window)?;
    Ok(window)
}

fn parse_selected_ion_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<SelectedIonList, ParseError> {
    let mut list = SelectedIonList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::SelectedIon {
            return Ok(false);
        }
        if is_open {
            list.selected_ions.push(parse_ion_selection(ws, &element)?);
        } else {
            list.selected_ions.push(SelectedIon::default());
        }
        Ok(true)
    })?;
    Ok(list)
}

fn parse_activation<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Activation, ParseError> {
    let mut activation = Activation::default();
    ws.collect_params_into(start, &mut activation)?;
    Ok(activation)
}

fn parse_ion_selection<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<SelectedIon, ParseError> {
    let mut ion = SelectedIon::default();
    ws.collect_params_into(start, &mut ion)?;
    Ok(ion)
}

fn push_selected_ion_onto_precursor<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    element: &BytesStart<'_>,
    precursor: &mut Precursor,
) -> Result<(), ParseError> {
    let list = precursor
        .selected_ion_list
        .get_or_insert_with(Default::default);
    list.selected_ions.push(parse_ion_selection(ws, element)?);
    list.count = Some(list.selected_ions.len());
    Ok(())
}

fn append_empty_selected_ion(precursor: &mut Precursor) {
    let list = precursor
        .selected_ion_list
        .get_or_insert_with(Default::default);
    list.selected_ions.push(SelectedIon::default());
    list.count = Some(list.selected_ions.len());
}
