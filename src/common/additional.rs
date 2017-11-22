
use std::collections::HashMap;

#[derive(Debug)]
enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Data(Vec<u8>),
}

#[derive(Default, Debug)]
pub struct Additionals {
    // TODO: Int & Float types
    items: HashMap<String, Value>,
}

impl Additionals {
    pub fn new() -> Self {
        Default::default()
    }

    fn set(&mut self, key: &str, value: Value) {
        if self.items.insert(key.to_string(), value).is_some() {
            warn!("Overwriting additonals: key={}", key);
        }
    }

    pub fn set_str(&mut self, key: &str, value: String) {
        self.set(key, Value::Str(value));
    }

    pub fn set_int(&mut self, key: &str, value: i64) {
        self.set(key, Value::Int(value));
    }

    pub fn set_float(&mut self, key: &str, value: f64) {
        self.set(key, Value::Float(value));
    }

    pub fn set_bool(&mut self, key: &str, value: bool) {
        self.set(key, Value::Bool(value));
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn to_capnp(&self, builder: &mut ::common_capnp::additionals::Builder) {
        let mut items = builder.borrow().init_items(self.items.len() as u32);
        for (i, (key, value)) in self.items.iter().enumerate() {
            let mut item = items.borrow().get(i as u32);
            item.set_key(&key);
            match value {
                &Value::Str(ref v) => item.get_value().set_str(v),
                &Value::Int(v) => item.get_value().set_int(v),
                &Value::Float(v) => item.get_value().set_float(v),
                &Value::Bool(v) => item.get_value().set_bool(v),
                &Value::Data(ref v) => item.get_value().set_data(v),
            }

        }
    }

    pub fn get_string(&self, key: &str) -> Option<String> {
        match self.items.get(key) {
            Some(&Value::Str(ref s)) => Some(s.clone()),
            _ => None,
        }
    }

    fn value_from_capnp(reader: &::common_capnp::additionals::item::value::Reader) -> Value {
        match reader.which().unwrap() {
            ::common_capnp::additionals::item::value::Str(value) => Value::Str(
                value.unwrap().to_string(),
            ),
            ::common_capnp::additionals::item::value::Int(value) => Value::Int(value),
            ::common_capnp::additionals::item::value::Float(value) => Value::Float(value),
            ::common_capnp::additionals::item::value::Bool(value) => Value::Bool(value),
            ::common_capnp::additionals::item::value::Data(value) => Value::Data(
                value.unwrap().to_vec(),
            ),
        }
    }

    pub fn from_capnp(reader: &::common_capnp::additionals::Reader) -> Self {
        Additionals {
            items: reader
                .get_items()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r.get_key().unwrap().to_string(),
                        Additionals::value_from_capnp(&r.get_value()),
                    )
                })
                .collect(),
        }
    }
}
