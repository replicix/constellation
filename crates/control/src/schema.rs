//! JSON Schema for the whole control protocol.
//!
//! [`schema_document`] walks the method table ([`visit_all`]) and asks
//! `schemars` for the schema of every method's `Params`, `Result` and (for
//! subscriptions) `Event` type, plus the handshake and error envelopes. The
//! document is keyed by method name:
//!
//! ```json
//! { "methods": { "pin.add": { "min_role": "operator", "mutating": true,
//!                             "streaming": "none",
//!                             "params": {…}, "result": {…} }, … },
//!   "protocol": { "Hello": {…}, "Welcome": {…}, "ControlError": {…} },
//!   "$defs": { "StatusReport": {…}, … } }
//! ```
//!
//! All types share one `$defs` table, so a type used by ten methods is
//! described once.
//!
//! ## Keeping it current
//!
//! The committed copy lives at `crates/control/schema/control.schema.json`.
//! The `schema_file_is_current` test regenerates the document and fails if
//! the file differs; refresh it with
//!
//! ```text
//! CONSTELLATION_BLESS=1 cargo test -p constellation-control schema
//! ```
//!
//! Output is canonical (object keys sorted, two-space indent, trailing
//! newline), so it does not depend on hash order or on whether some other
//! crate enabled `serde_json/preserve_order`.
//!
//! ## TypeScript
//!
//! Generating TypeScript types for the UI is **plan 33's** choice (`ts-rs`,
//! `typeshare`, or `json-schema-to-typescript` over this file). This crate
//! deliberately adds none of them; the JSON Schema is the neutral artifact
//! any of those can consume.

use crate::methods::{visit_all, Method, MethodVisitor, StreamKind};
use crate::proto::{ControlError, Encoding, Hello, Welcome};
use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, SchemaGenerator};
use serde_json::{json, Map, Value};

/// Where the committed schema lives, relative to the crate root.
pub const SCHEMA_FILE: &str = "schema/control.schema.json";

/// Environment variable that makes the staleness test rewrite the file.
pub const BLESS_VAR: &str = "CONSTELLATION_BLESS";

struct Collector {
    generator: SchemaGenerator,
    methods: Map<String, Value>,
}

impl Collector {
    fn schema_of<T: JsonSchema>(&mut self) -> Value {
        serde_json::to_value(self.generator.subschema_for::<T>()).unwrap_or(Value::Null)
    }
}

impl MethodVisitor for Collector {
    fn visit<M: Method>(&mut self) {
        let mut entry = Map::new();
        entry.insert("min_role".into(), json!(M::MIN_ROLE));
        entry.insert("mutating".into(), json!(M::MUTATING));
        entry.insert("streaming".into(), json!(M::STREAMING));
        entry.insert("params".into(), self.schema_of::<M::Params>());
        entry.insert("result".into(), self.schema_of::<M::Result>());
        if M::STREAMING == StreamKind::Events {
            entry.insert("event".into(), self.schema_of::<M::Event>());
        }
        self.methods
            .insert(M::NAME.to_string(), Value::Object(entry));
    }
}

/// The schema document for every method and the protocol envelopes.
pub fn schema_document() -> Value {
    let mut collector = Collector {
        generator: SchemaGenerator::new(SchemaSettings::draft2020_12()),
        methods: Map::new(),
    };
    visit_all(&mut collector);
    let mut protocol = Map::new();
    protocol.insert("Hello".into(), collector.schema_of::<Hello>());
    protocol.insert("Welcome".into(), collector.schema_of::<Welcome>());
    protocol.insert("ControlError".into(), collector.schema_of::<ControlError>());
    protocol.insert("Encoding".into(), collector.schema_of::<Encoding>());
    let defs = collector.generator.take_definitions(true);
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "constellation control protocol",
        "description": "Generated from constellation-control's method table; do not edit. \
                        Regenerate with CONSTELLATION_BLESS=1 cargo test -p constellation-control schema.",
        "methods": collector.methods,
        "protocol": protocol,
        "$defs": defs,
    })
}

fn sorted(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            let mut out = Map::new();
            let mut map = map;
            for key in keys {
                if let Some(v) = map.remove(&key) {
                    out.insert(key, sorted(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sorted).collect()),
        other => other,
    }
}

/// [`schema_document`] rendered canonically (sorted keys, 2-space indent,
/// trailing newline): the exact bytes of the committed file.
pub fn schema_json() -> String {
    let mut text = serde_json::to_string_pretty(&sorted(schema_document()))
        .unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"));
    text.push('\n');
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::methods::METHODS;
    use std::path::PathBuf;

    fn file() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(SCHEMA_FILE)
    }

    #[test]
    fn every_method_has_params_and_result() {
        let doc = schema_document();
        let methods = doc["methods"].as_object().unwrap();
        assert_eq!(methods.len(), METHODS.len());
        for m in METHODS {
            let entry = &methods[m.name];
            assert!(
                entry["params"].is_object() || entry["params"].is_boolean(),
                "{}",
                m.name
            );
            assert!(entry["result"].is_object(), "{}", m.name);
            assert_eq!(entry["streaming"], json!(m.streaming), "{}", m.name);
            assert_eq!(
                entry["event"].is_object(),
                m.streaming == StreamKind::Events,
                "{}",
                m.name
            );
        }
        // Spot checks that the shapes are the real ones.
        let mount = &doc["$defs"]["ViewMountParams"]["properties"];
        for field in ["subtree", "source", "labels", "qos", "confine_links"] {
            assert!(mount[field].is_object(), "view.mount.{field}");
        }
        assert!(doc["$defs"]["StatusReport"].is_object());
        assert!(doc["protocol"]["ControlError"].is_object());
    }

    /// `$ref` → its definition in `defs`.
    fn resolve<'a>(schema: &'a Value, defs: &'a Value) -> &'a Value {
        match schema.get("$ref").and_then(Value::as_str) {
            Some(r) => {
                let name = r.strip_prefix("#/$defs/").expect("local $ref");
                resolve(&defs[name], defs)
            }
            None => schema,
        }
    }

    /// A JSON instance of `schema`. Odd `pick`s fill *every* property
    /// (optional ones included, so `Option`s are `Some` and defaults are
    /// overridden); even ones only the required properties (so serde fills
    /// in the defaults and `None`s). Where there is a choice (`oneOf`,
    /// `enum`) it takes alternative number `pick`, so iterating `pick`
    /// visits every enum variant.
    fn sample(schema: &Value, defs: &Value, pick: usize, depth: usize) -> Value {
        assert!(depth < 40, "runaway recursion in the schema");
        let schema = resolve(schema, defs);
        if schema == &Value::Bool(true) || schema.as_object().is_some_and(|o| o.is_empty()) {
            // Free-form JSON (`JsonValue`): something nested.
            return json!({"k": [1, "v", null, {"n": 2.5}]});
        }
        if let Some(c) = schema.get("const") {
            return c.clone();
        }
        if let Some(options) = schema.get("enum").and_then(Value::as_array) {
            return options[pick % options.len()].clone();
        }
        for key in ["oneOf", "anyOf"] {
            if let Some(options) = schema.get(key).and_then(Value::as_array) {
                let real: Vec<&Value> = options
                    .iter()
                    .filter(|o| o.get("type") != Some(&json!("null")))
                    .collect();
                return sample(real[pick % real.len()], defs, pick, depth + 1);
            }
        }
        let ty = match &schema["type"] {
            Value::String(t) => t.as_str(),
            Value::Array(ts) => ts
                .iter()
                .filter_map(Value::as_str)
                .find(|t| *t != "null")
                .expect("a non-null type"),
            other => panic!("no type in {schema} ({other})"),
        };
        match ty {
            "object" => {
                let mut out = Map::new();
                let required = |name: &str| {
                    schema["required"]
                        .as_array()
                        .is_some_and(|r| r.iter().any(|n| n == name))
                };
                if let Some(props) = schema.get("properties").and_then(Value::as_object) {
                    for (name, prop) in props {
                        if pick % 2 == 1 || required(name) {
                            out.insert(name.clone(), sample(prop, defs, pick, depth + 1));
                        }
                    }
                }
                if let Some(extra) = schema.get("additionalProperties").filter(|v| v.is_object()) {
                    out.insert("key".into(), sample(extra, defs, pick, depth + 1));
                }
                Value::Object(out)
            }
            "array" => match schema.get("prefixItems").and_then(Value::as_array) {
                Some(items) => items
                    .iter()
                    .map(|i| sample(i, defs, pick, depth + 1))
                    .collect(),
                None => Value::Array(vec![
                    sample(&schema["items"], defs, pick, depth + 1),
                    sample(&schema["items"], defs, pick + 2, depth + 1),
                ]),
            },
            "string" if schema.get("contentEncoding") == Some(&json!("base64")) => {
                json!("AP8H")
            }
            "string" => json!(format!("s{pick}")),
            "integer" => json!(7 + pick as u64),
            "number" => json!(1.5),
            "boolean" => json!(true),
            other => panic!("unhandled type {other}"),
        }
    }

    /// Decode `sample` as `T` and push it through both encodings.
    fn round_trips<T>(what: &str, sample: Value)
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let value: T = serde_json::from_value(sample.clone())
            .unwrap_or_else(|e| panic!("{what}: schema sample {sample} does not decode: {e}"));
        let expect = serde_json::to_value(&value).unwrap();
        for enc in crate::proto::SUPPORTED_ENCODINGS {
            let blob = crate::proto::Blob::encode(enc, &value)
                .unwrap_or_else(|e| panic!("{what}: {enc:?} encode: {e}"));
            let back: T = blob
                .decode()
                .unwrap_or_else(|e| panic!("{what}: {enc:?} decode: {e}"));
            assert_eq!(
                serde_json::to_value(&back).unwrap(),
                expect,
                "{what}: {enc:?} round trip changed the value"
            );
        }
    }

    struct RoundTrip<'a> {
        doc: &'a Value,
    }

    impl MethodVisitor for RoundTrip<'_> {
        fn visit<M: Method>(&mut self) {
            let defs = &self.doc["$defs"];
            let entry = &self.doc["methods"][M::NAME];
            for pick in 0..16 {
                let what = |part: &str| format!("{} {part} (pick {pick})", M::NAME);
                round_trips::<M::Params>(&what("params"), sample(&entry["params"], defs, pick, 0));
                round_trips::<M::Result>(&what("result"), sample(&entry["result"], defs, pick, 0));
                if M::STREAMING == StreamKind::Events {
                    round_trips::<M::Event>(&what("event"), sample(&entry["event"], defs, pick, 0));
                }
            }
        }
    }

    /// Postcard is positional and not self-describing: a type that only
    /// works in JSON (a `skip_serializing_if`, an untagged enum, a bare
    /// `serde_json::Value`, a foreign type with a clever `Deserialize`)
    /// surfaces here, for every parameter, result and event type of the
    /// whole table, with every optional field populated and every enum
    /// variant visited.
    #[test]
    fn every_method_type_round_trips_in_both_encodings() {
        let doc = schema_document();
        visit_all(&mut RoundTrip { doc: &doc });
        // The error envelope travels inside postcard `Response`s too.
        for pick in 0..8 {
            round_trips::<ControlError>(
                "ControlError",
                sample(&doc["protocol"]["ControlError"], &doc["$defs"], pick, 0),
            );
        }
    }

    fn mentions_write_only(schema: &Value, defs: &Value, seen: &mut Vec<String>) -> bool {
        match schema {
            Value::Object(map) => {
                if map.get("writeOnly") == Some(&Value::Bool(true)) {
                    return true;
                }
                if let Some(r) = map.get("$ref").and_then(Value::as_str) {
                    let name = r.trim_start_matches("#/$defs/").to_string();
                    if seen.contains(&name) {
                        return false;
                    }
                    seen.push(name.clone());
                    if mentions_write_only(&defs[&name], defs, seen) {
                        return true;
                    }
                }
                map.values().any(|v| mentions_write_only(v, defs, seen))
            }
            Value::Array(items) => items.iter().any(|v| mentions_write_only(v, defs, seen)),
            _ => false,
        }
    }

    /// `secret_params` (which withholds the audit digest) is set for
    /// exactly the methods whose params contain a `Secret` somewhere.
    #[test]
    fn secret_params_matches_the_params_schema() {
        let doc = schema_document();
        for m in METHODS {
            let params = &doc["methods"][m.name]["params"];
            let has_secret = mentions_write_only(params, &doc["$defs"], &mut Vec::new());
            assert_eq!(m.secret_params, has_secret, "{}", m.name);
        }
        assert!(METHODS.iter().any(|m| m.secret_params));
    }

    #[test]
    fn generation_is_deterministic() {
        assert_eq!(schema_json(), schema_json());
    }

    #[test]
    fn schema_file_is_current() {
        let fresh = schema_json();
        let path = file();
        if std::env::var_os(BLESS_VAR).is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &fresh).unwrap();
            return;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            committed == fresh,
            "{} is stale (or missing). Regenerate it with:\n    \
             {BLESS_VAR}=1 cargo test -p constellation-control schema",
            path.display()
        );
    }
}
