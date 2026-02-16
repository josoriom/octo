use crate::{
    Product, ProductList,
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    mzml::{
        attr_meta::{
            ACC_ATTR_EXTERNAL_SPECTRUM_ID, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPECTRUM_REF,
        },
        schema::TagId,
        structs::IsolationWindow,
    },
};

#[inline]
pub fn parse_product_list(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
) -> Option<ProductList> {
    let list_id = children_lookup.first_id(spectrum_id, TagId::ProductList)?;
    let product_ids = children_lookup.ids_for(list_id, TagId::Product);

    if product_ids.is_empty() {
        return None;
    }

    let products = product_ids
        .iter()
        .map(|&id| {
            let rows = owner_rows.get(id);
            let (cv_params, user_params) =
                parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));

            Product {
                spectrum_ref: get_attr_text(rows, ACC_ATTR_SPECTRUM_REF),
                source_file_ref: get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF),
                external_spectrum_id: get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID),
                isolation_window: parse_isolation_window(owner_rows, children_lookup, id),
                cv_params,
                user_params,
            }
        })
        .collect::<Vec<_>>();

    let (cv_params, user_params) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, list_id));

    Some(ProductList {
        count: Some(products.len()),
        products,
        cv_params,
        user_params,
    })
}

#[inline]
fn parse_isolation_window(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    product_id: u32,
) -> Option<IsolationWindow> {
    let window_id = children_lookup.first_id(product_id, TagId::IsolationWindow)?;
    let (cv_params, user_params) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, window_id));

    Some(IsolationWindow {
        cv_params,
        user_params,
        referenceable_param_group_refs: Vec::new(),
    })
}
