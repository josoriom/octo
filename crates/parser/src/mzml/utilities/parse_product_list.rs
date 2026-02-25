use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::mzml::{
    schema::TagId,
    structs::*,
    utilities::{
        ParseError, attr, attr_usize, parse_isolation_window, parsing_workspace::ParsingWorkspace,
    },
};

pub(crate) fn parse_product_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<ProductList, ParseError> {
    let mut list = ProductList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::Product {
            return Ok(false);
        }
        if is_open {
            list.products.push(parse_product(ws, &element)?);
        } else {
            list.products.push(Product {
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

pub(crate) fn parse_product<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<Product, ParseError> {
    let mut product = Product {
        spectrum_ref: attr(start, b"spectrumRef"),
        source_file_ref: attr(start, b"sourceFileRef"),
        external_spectrum_id: attr(start, b"externalSpectrumID"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag == TagId::IsolationWindow && is_open {
            product.isolation_window = Some(parse_isolation_window(ws, &element)?);
            Ok(true)
        } else {
            Ok(false)
        }
    })?;
    Ok(product)
}
