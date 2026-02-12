use crate::{
    Product, ProductList,
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{schema::TagId, structs::IsolationWindow},
};

/// <productList>
#[inline]
pub fn parse_product_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<ProductList> {
    let mut owner_rows: OwnerRows<'_> = OwnerRows::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.id).or_default().push(m);

        match m.tag_id {
            TagId::ProductList => {
                if list_id.is_none() {
                    list_id = Some(m.id);
                }
            }
            TagId::Product => {
                if fallback_list_id.is_none() && m.parent_index != 0 {
                    fallback_list_id = Some(m.parent_index);
                }
            }
            _ => {}
        }
    }

    let list_id = list_id.or(fallback_list_id)?;
    let product_ids = children_lookup.ids_for(metadata, list_id, TagId::Product);
    if product_ids.is_empty() {
        return None;
    }

    let mut products = Vec::with_capacity(product_ids.len());
    for product_id in product_ids {
        products.push(parse_product(
            metadata,
            children_lookup,
            &owner_rows,
            product_id,
        ));
    }

    Some(ProductList {
        count: Some(products.len()),
        products,
    })
}

/// <product>
#[inline]
fn parse_product<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    product_id: u32,
) -> Product {
    let product_parent = ChildrenLookup::rows_for_owner(owner_rows, product_id)
        .first()
        .map(|m| m.parent_index)
        .unwrap_or(0);

    let isolation_window_id = children_lookup
        .first_id(product_id, TagId::IsolationWindow)
        .or_else(|| children_lookup.first_id(product_parent, TagId::IsolationWindow));

    Product {
        spectrum_ref: None,
        source_file_ref: None,
        external_spectrum_id: None,
        isolation_window: parse_isolation_window(
            metadata,
            children_lookup,
            owner_rows,
            isolation_window_id,
            product_id,
            product_parent,
        ),
    }
}

/// <isolationWindow>
#[inline]
fn parse_isolation_window<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    isolation_window_id: Option<u32>,
    product_id: u32,
    product_parent: u32,
) -> Option<IsolationWindow> {
    let pick = |id| children_lookup.param_rows(metadata, owner_rows, id);

    let mut meta = match isolation_window_id {
        Some(id) => pick(id),
        None => pick(product_id),
    };

    if meta.is_empty() && product_parent != 0 && product_parent != product_id {
        meta = pick(product_parent);
    }

    if meta.is_empty() && isolation_window_id.is_none() {
        return None;
    }

    let (cv_params, user_params) = parse_cv_and_user_params(&meta);

    Some(IsolationWindow {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    })
}
