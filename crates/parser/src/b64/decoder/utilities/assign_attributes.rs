use crate::b64::attr_meta::{AccessionTail, CV_REF_ATTR, key_to_attr_tail};
use crate::decoder::decode::{Metadatum, MetadatumValue};
use crate::mzml::schema::{SchemaNode, TagId, schema};
use hashbrown::HashMap;
use serde::Serialize;
use serde_json::Value;
use std::sync::OnceLock;

static ALL_TAG_SPECS: OnceLock<HashMap<TagId, Vec<FieldSpec>>> = OnceLock::new();
static XMLKEY_TO_TAIL: OnceLock<HashMap<String, AccessionTail>> = OnceLock::new();

#[derive(Clone)]
struct FieldSpec {
    tail: AccessionTail,
    field_key: String,
    camel_key: String,
    id_variant: Option<String>,
    uri_variant: Option<String>,
}

pub(crate) fn assign_attributes<T: Serialize>(
    value: &T,
    tag_id: TagId,
    node_id: u32,
    parent_id: u32,
) -> Vec<Metadatum> {
    let specs = tag_specs_for(tag_id);
    if specs.is_empty() {
        return Vec::new();
    }

    let json = serde_json::to_value(value).expect("assign_attributes: serialization failed");
    let obj = json
        .as_object()
        .expect("assign_attributes: value must serialize to a JSON object");

    let mut out = Vec::with_capacity(specs.len());
    let mut item_index: u32 = 0;

    for spec in specs {
        let Some(json_value) = find_field_in_object(obj, spec) else {
            continue;
        };
        let Some(metadatum_value) = json_value_to_metadatum_value(json_value) else {
            continue;
        };

        let accession = format_accession(spec.tail);
        out.push(Metadatum {
            item_index,
            id: node_id,
            parent_id,
            tag_id,
            accession: Some(accession),
            unit_accession: None,
            value: metadatum_value,
        });
        item_index += 1;
    }

    out
}

fn tag_specs_for(tag_id: TagId) -> &'static [FieldSpec] {
    ALL_TAG_SPECS
        .get_or_init(build_all_tag_specs)
        .get(&tag_id)
        .map(|v| v.as_slice())
        .unwrap_or(&[])
}

fn build_all_tag_specs() -> HashMap<TagId, Vec<FieldSpec>> {
    let xmlkey_to_tail = XMLKEY_TO_TAIL.get_or_init(build_xmlkey_to_tail_map);
    let schema_tree = schema();
    let mut all_specs: HashMap<TagId, Vec<FieldSpec>> = HashMap::new();

    for root in schema_tree.roots.values() {
        collect_specs_from_node(root, xmlkey_to_tail, &mut all_specs);
    }

    all_specs
}

fn collect_specs_from_node(
    node: &SchemaNode,
    xmlkey_to_tail: &HashMap<String, AccessionTail>,
    all_specs: &mut HashMap<TagId, Vec<FieldSpec>>,
) {
    for &tag_id in &node.self_tags {
        let specs = all_specs.entry(tag_id).or_default();
        for (field_key, xml_keys) in node.attributes.iter() {
            let Some(tail) = xml_keys.iter().find_map(|k| xmlkey_to_tail.get(k)) else {
                continue;
            };
            if specs.iter().any(|s| s.tail.raw() == tail.raw()) {
                continue;
            }
            let (camel_key, id_variant, uri_variant) = snake_to_camel_variants(field_key);
            specs.push(FieldSpec {
                tail: *tail,
                field_key: field_key.clone(),
                camel_key,
                id_variant,
                uri_variant,
            });
        }
        specs.sort_by_key(|s| s.tail.raw());
    }

    for child in node.children.values() {
        collect_specs_from_node(child, xmlkey_to_tail, all_specs);
    }
}

fn build_xmlkey_to_tail_map() -> HashMap<String, AccessionTail> {
    let schema_tree = schema();
    let mut xml_keys: Vec<String> = Vec::new();

    for root in schema_tree.roots.values() {
        collect_xml_keys_from_node(root, &mut xml_keys);
    }

    xml_keys.sort();
    xml_keys.dedup();

    xml_keys
        .into_iter()
        .filter_map(|key| {
            let tail = key_to_attr_tail(&key)?;
            Some((key, tail))
        })
        .collect()
}

fn collect_xml_keys_from_node(node: &SchemaNode, out: &mut Vec<String>) {
    for xml_keys in node.attributes.values() {
        out.extend(xml_keys.iter().cloned());
    }
    for child in node.children.values() {
        collect_xml_keys_from_node(child, out);
    }
}

fn find_field_in_object<'a>(
    obj: &'a serde_json::Map<String, Value>,
    spec: &FieldSpec,
) -> Option<&'a Value> {
    obj.get(&spec.field_key)
        .or_else(|| obj.get(&spec.camel_key))
        .or_else(|| spec.id_variant.as_ref().and_then(|k| obj.get(k)))
        .or_else(|| spec.uri_variant.as_ref().and_then(|k| obj.get(k)))
}

fn json_value_to_metadatum_value(v: &Value) -> Option<MetadatumValue> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(MetadatumValue::Text(s.clone())),
        Value::Bool(b) => Some(MetadatumValue::Text(b.to_string())),
        Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                assert!(
                    u <= (1u64 << 53),
                    "numeric attribute too large for lossless f64: {u}"
                );
                Some(MetadatumValue::Number(u as f64))
            } else if let Some(i) = n.as_i64() {
                assert!(
                    (i.unsigned_abs()) <= (1u64 << 53),
                    "numeric attribute too large for lossless f64: {i}"
                );
                Some(MetadatumValue::Number(i as f64))
            } else {
                n.as_f64().map(MetadatumValue::Number)
            }
        }
        other => panic!("non-primitive attribute value not supported: {other:?}"),
    }
}

fn snake_to_camel_variants(snake: &str) -> (String, Option<String>, Option<String>) {
    let mut parts = snake.split('_').filter(|p| !p.is_empty());
    let first = match parts.next() {
        Some(f) => f,
        None => return (String::new(), None, None),
    };

    let mut camel = String::from(first);
    for part in parts {
        let mut chars = part.chars();
        if let Some(first_char) = chars.next() {
            for upper in first_char.to_uppercase() {
                camel.push(upper);
            }
            camel.push_str(chars.as_str());
        }
    }

    let id_variant = camel.ends_with("Id").then(|| {
        let mut v = camel[..camel.len() - 2].to_string();
        v.push_str("ID");
        v
    });

    let uri_variant = camel.ends_with("Uri").then(|| {
        let mut v = camel[..camel.len() - 3].to_string();
        v.push_str("URI");
        v
    });

    (camel, id_variant, uri_variant)
}

fn format_accession(tail: AccessionTail) -> String {
    use core::fmt::Write;
    let mut accession = String::with_capacity(CV_REF_ATTR.len() + 8);
    accession.push_str(CV_REF_ATTR);
    accession.push(':');
    write!(&mut accession, "{:07}", tail.raw()).expect("format accession tail");
    accession
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_to_camel_produces_correct_camel_case() {
        let (camel, id_variant, uri_variant) = snake_to_camel_variants("default_array_length");
        assert_eq!(camel, "defaultArrayLength");
        assert!(id_variant.is_none());
        assert!(uri_variant.is_none());
    }

    #[test]
    fn snake_to_camel_detects_id_suffix_variant() {
        let (camel, id_variant, uri_variant) = snake_to_camel_variants("native_id");
        assert_eq!(camel, "nativeId");
        assert_eq!(id_variant.as_deref(), Some("nativeID"));
        assert!(uri_variant.is_none());
    }

    #[test]
    fn snake_to_camel_detects_uri_suffix_variant() {
        let (camel, id_variant, uri_variant) = snake_to_camel_variants("cv_uri");
        assert_eq!(camel, "cvUri");
        assert!(id_variant.is_none());
        assert_eq!(uri_variant.as_deref(), Some("cvURI"));
    }

    #[test]
    fn snake_to_camel_handles_empty_input() {
        let (camel, id_variant, uri_variant) = snake_to_camel_variants("");
        assert!(camel.is_empty());
        assert!(id_variant.is_none());
        assert!(uri_variant.is_none());
    }

    #[test]
    fn json_value_to_metadatum_value_handles_null_as_none() {
        assert!(json_value_to_metadatum_value(&Value::Null).is_none());
    }

    #[test]
    fn json_value_to_metadatum_value_handles_string() {
        let result = json_value_to_metadatum_value(&Value::String("hello".to_string()));
        assert!(matches!(result, Some(MetadatumValue::Text(s)) if s == "hello"));
    }

    #[test]
    fn json_value_to_metadatum_value_handles_bool_as_text() {
        let result = json_value_to_metadatum_value(&Value::Bool(true));
        assert!(matches!(result, Some(MetadatumValue::Text(s)) if s == "true"));
    }

    #[test]
    fn json_value_to_metadatum_value_handles_integer() {
        let result = json_value_to_metadatum_value(&serde_json::json!(42u64));
        assert!(
            matches!(result, Some(MetadatumValue::Number(n)) if (n - 42.0).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn find_field_in_object_prefers_snake_case_key() {
        let obj: serde_json::Map<String, Value> =
            serde_json::from_str(r#"{"my_field": "snake", "myField": "camel"}"#).unwrap();
        let spec = FieldSpec {
            tail: AccessionTail::from_raw(1),
            field_key: "my_field".to_string(),
            camel_key: "myField".to_string(),
            id_variant: None,
            uri_variant: None,
        };
        let found = find_field_in_object(&obj, &spec);
        assert_eq!(found.and_then(|v| v.as_str()), Some("snake"));
    }

    #[test]
    fn find_field_in_object_falls_back_to_camel_key() {
        let obj: serde_json::Map<String, Value> =
            serde_json::from_str(r#"{"myField": "camel"}"#).unwrap();
        let spec = FieldSpec {
            tail: AccessionTail::from_raw(1),
            field_key: "my_field".to_string(),
            camel_key: "myField".to_string(),
            id_variant: None,
            uri_variant: None,
        };
        let found = find_field_in_object(&obj, &spec);
        assert_eq!(found.and_then(|v| v.as_str()), Some("camel"));
    }

    #[test]
    fn find_field_in_object_returns_none_when_no_key_matches() {
        let obj: serde_json::Map<String, Value> =
            serde_json::from_str(r#"{"other": "value"}"#).unwrap();
        let spec = FieldSpec {
            tail: AccessionTail::from_raw(1),
            field_key: "my_field".to_string(),
            camel_key: "myField".to_string(),
            id_variant: None,
            uri_variant: None,
        };
        assert!(find_field_in_object(&obj, &spec).is_none());
    }

    #[test]
    fn format_accession_produces_correct_string() {
        let tail = AccessionTail::from_raw(1000511);
        let accession = format_accession(tail);
        assert!(accession.ends_with(":1000511"));
        assert!(accession.starts_with(CV_REF_ATTR));
    }
}
