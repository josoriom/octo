use crate::{
    Product, ProductList,
    b64::{
        attr_meta::{
            ACC_ATTR_EXTERNAL_SPECTRUM_ID, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPECTRUM_REF,
        },
        utilities::{
            children_lookup::{ChildrenLookup, DefaultMetadataPolicy, OwnerRows},
            common::get_attr_text,
            parse_cv_and_user_params,
        },
    },
    decoder::decode::Metadatum,
    mzml::{schema::TagId, structs::IsolationWindow},
};

#[inline]
pub(crate) fn parse_product_list<'a>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
) -> Option<ProductList> {
    let list_id = children_lookup
        .ids_for(spectrum_id, TagId::ProductList)
        .first()
        .copied()?;

    let product_ids = children_lookup.ids_for(list_id, TagId::Product);
    if product_ids.is_empty() {
        return None;
    }

    let policy = DefaultMetadataPolicy;
    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let products = product_ids
        .iter()
        .map(|&product_id| {
            let rows = owner_rows.get(product_id);

            param_buffer.clear();
            children_lookup.get_param_rows_into(owner_rows, product_id, &policy, &mut param_buffer);
            let (cv_params, user_params) = parse_cv_and_user_params(&param_buffer);

            Product {
                spectrum_ref: get_attr_text(rows, ACC_ATTR_SPECTRUM_REF),
                source_file_ref: get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF),
                external_spectrum_id: get_attr_text(rows, ACC_ATTR_EXTERNAL_SPECTRUM_ID),
                isolation_window: parse_isolation_window(
                    owner_rows,
                    children_lookup,
                    product_id,
                    &policy,
                    &mut param_buffer,
                ),
                cv_params,
                user_params,
            }
        })
        .collect::<Vec<_>>();

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, list_id, &policy, &mut param_buffer);
    let (cv_params, user_params) = parse_cv_and_user_params(&param_buffer);

    Some(ProductList {
        count: Some(products.len()),
        products,
        cv_params,
        user_params,
    })
}

#[inline]
fn parse_isolation_window<'a>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    product_id: u32,
    policy: &DefaultMetadataPolicy,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Option<IsolationWindow> {
    let isolation_window_id = children_lookup
        .ids_for(product_id, TagId::IsolationWindow)
        .first()
        .copied()?;

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, isolation_window_id, policy, param_buffer);
    let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);

    Some(IsolationWindow {
        cv_params,
        user_params,
        referenceable_param_group_refs: Vec::new(),
    })
}
