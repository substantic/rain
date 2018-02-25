use std::collections::HashMap;
use errors::Result;
use std::error::Error;

#[derive(Default, Debug)]
pub struct Attributes {
    // TODO: Int & Float types
    items: HashMap<String, String>,
}

impl Attributes {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn find<'a, D>(&'a self, key: &str) -> Result<Option<D>>
    where
        D: ::serde::de::Deserialize<'a>,
    {
        match self.items.get(key) {
            Some(ref value) => ::serde_json::from_str(value).map(|v| Some(v)).map_err(|e| {
                format!("Error in parsing attribute '{}': {}", key, e.description()).into()
            }),
            None => Ok(None),
        }
    }

    pub fn get<'a, D>(&'a self, key: &str) -> Result<D>
    where
        D: ::serde::de::Deserialize<'a>,
    {
        match self.items.get(key) {
            Some(ref value) => ::serde_json::from_str(value).map_err(|e| {
                format!(
                    "Error in parsing attribute '{}': {} (data {:?})",
                    key,
                    e.description(),
                    &value
                ).into()
            }),
            None => {
                bail!("Key not found in attributes");
            }
        }
    }

    pub fn set<S>(&mut self, key: &str, value: S) -> Result<()>
    where
        S: ::serde::ser::Serialize,
    {
        self.items
            .insert(key.to_string(), ::serde_json::to_string(&value)?);
        Ok(())
    }

    pub fn to_capnp(&self, builder: &mut ::common_capnp::attributes::Builder) {
        let mut items = builder.borrow().init_items(self.items.len() as u32);
        for (i, (key, value)) in self.items.iter().enumerate() {
            let mut item = items.borrow().get(i as u32);
            item.set_key(&key);
            item.set_value(&value);
        }
    }

    pub fn from_capnp(reader: &::common_capnp::attributes::Reader) -> Self {
        let mut attrs = Attributes::new();
        attrs.update_from_capnp(reader);
        attrs
    }

    pub fn update_from_capnp(&mut self, reader: &::common_capnp::attributes::Reader) {
        for item in reader.get_items().unwrap() {
            let key = item.get_key().unwrap().to_string();
            let value = item.get_value().unwrap().into();
            self.items.insert(key, value);
        }
    }

    pub fn update(&mut self, attributes: Attributes) {
        for (k, v) in attributes.items {
            self.items.insert(k, v);
        }
    }

    pub fn as_hashmap(&self) -> &HashMap<String, String> {
        &self.items
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }
}
