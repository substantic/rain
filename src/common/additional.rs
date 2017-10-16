
use std::collections::HashMap;

#[derive(Default, Debug)]
pub struct Additional {
    // TODO: Int & Float types
    items: HashMap<String, String>
}

// TODO: Rename to Additionals
impl Additional {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_str(&mut self, key: &str, value: String) {
        if self.items.insert(key.to_string(), value).is_some() {
            warn!("Overwriting additonals: key={}", key);
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn to_capnp(&self, builder: &mut ::common_capnp::additional::Builder) {
        let mut items = builder.borrow().init_items(self.items.len() as u32);
        for (i, pair) in self.items.iter().enumerate() {
            let mut item = items.borrow().get(i as u32);
            item.set_key(&pair.0);
            item.get_value().set_text(&pair.1);
        }
    }

    pub fn get_string(&self, key: &str) -> Option<String> {
        self.items.get(key).map(|v| v.clone())
    }

    fn value_from_capnp(reader: &::common_capnp::additional::item::value::Reader) -> String {
        match reader.which().unwrap() {
            ::common_capnp::additional::item::value::Text(text) => text.unwrap().to_string(),
            _ => unimplemented!()
        }
    }

    pub fn from_capnp(reader: & ::common_capnp::additional::Reader) -> Self {
        Additional {
            items: reader.get_items().unwrap().iter()
                .map(|r|
                    (r.get_key().unwrap().to_string(), Additional::value_from_capnp(&r.get_value())))
                .collect()
        }
    }
}