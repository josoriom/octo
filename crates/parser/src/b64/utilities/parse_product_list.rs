use std::collections::HashMap;

use crate::{
    Product, ProductList,
    b64::utilities::{
        common::{ChildIndex, OwnerRows, ParseCtx, child_params_for_parent, ids_for_parent},
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{schema::TagId, structs::IsolationWindow},
};

/// <productList>
#[inline]
pub fn parse_product_list(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<ProductList> {
    let mut owner_rows: OwnerRows<'_> = HashMap::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);

        match m.tag_id {
            TagId::ProductList => {
                if list_id.is_none() {
                    list_id = Some(m.owner_id);
                }
            }
            TagId::Product => {
                if fallback_list_id.is_none() {
                    fallback_list_id = Some(m.parent_index);
                }
            }
            _ => {}
        }
    }

    let list_id = list_id.or(fallback_list_id)?;
    let ctx = ParseCtx {
        metadata,
        child_index,
        owner_rows: &owner_rows,
    };

    let product_ids = ids_for_parent(&ctx, list_id, TagId::Product);
    if product_ids.is_empty() {
        return None;
    }

    let mut products = Vec::with_capacity(product_ids.len());
    for product_id in product_ids {
        products.push(parse_product(&ctx, product_id));
    }

    Some(ProductList {
        count: Some(products.len()),
        products,
    })
}

/// <product>
#[inline]
fn parse_product(ctx: &ParseCtx<'_>, product_id: u32) -> Product {
    let product_parent = rows_for_owner(ctx.owner_rows, product_id)
        .first()
        .map(|m| m.parent_index)
        .unwrap_or(0);

    let isolation_window_id = ctx
        .child_index
        .first_id(product_id, TagId::IsolationWindow)
        .or_else(|| {
            ctx.child_index
                .first_id(product_parent, TagId::IsolationWindow)
        });

    Product {
        spectrum_ref: None,
        source_file_ref: None,
        external_spectrum_id: None,
        isolation_window: parse_isolation_window(
            ctx,
            isolation_window_id,
            product_id,
            product_parent,
        ),
    }
}

/// <isolationWindow>
#[inline]
fn parse_isolation_window(
    ctx: &ParseCtx<'_>,
    isolation_window_id: Option<u32>,
    product_id: u32,
    product_parent: u32,
) -> Option<IsolationWindow> {
    let mut meta = if let Some(iso_id) = isolation_window_id {
        child_params_for_parent(ctx.owner_rows, ctx.child_index, iso_id)
    } else {
        child_params_for_parent(ctx.owner_rows, ctx.child_index, product_id)
    };

    if meta.is_empty() && product_parent != product_id {
        meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, product_parent);
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

#[inline]
fn rows_for_owner<'a>(
    owner_rows: &'a HashMap<u32, Vec<&'a Metadatum>>,
    owner_id: u32,
) -> &'a [&'a Metadatum] {
    owner_rows
        .get(&owner_id)
        .map(|v| v.as_slice())
        .unwrap_or(&[])
}
