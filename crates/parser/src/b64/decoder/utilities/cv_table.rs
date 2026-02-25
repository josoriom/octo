use hashbrown::HashMap;
use once_cell::sync::Lazy;
use serde_json::Value;

static RAW_JSON: &str = include_str!("cv_table.json");

pub(crate) static TABLE: Lazy<HashMap<String, Value>> = Lazy::new(|| {
    let v: Value = serde_json::from_str(RAW_JSON).unwrap();
    let mut map: HashMap<String, Value> = HashMap::new();

    if let Value::Object(obj) = v {
        for (k, val) in obj {
            map.insert(k, val);
        }
    }

    map
});

pub(crate) fn get(key: &str) -> Option<&Value> {
    TABLE.get(key)
}
