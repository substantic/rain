
#[derive(Default, Debug)]
pub struct Additional {
    // TODO: Int & Float types
    items: Vec<(String, String)>
}

impl Additional {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_str(&mut self, key: &str, value: String) {
        self.items.push((key.to_string(), value))
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
}