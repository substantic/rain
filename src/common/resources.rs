#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    pub cpus: u32,
}

impl Resources {
    pub fn cpus(cpus: u32) -> Self {
        Resources { cpus }
    }

    pub fn add(&mut self, resources: &Resources) {
        self.cpus += resources.cpus;
    }

    pub fn remove(&mut self, resources: &Resources) {
        assert!(self.cpus >= resources.cpus);
        self.cpus -= resources.cpus;
    }

    pub fn from_capnp(reader: &::capnp_gen::common_capnp::resources::Reader) -> Self {
        Resources { cpus: reader.get_n_cpus() }
    }

    pub fn to_capnp(&self, builder: &mut ::capnp_gen::common_capnp::resources::Builder) {
        builder.set_n_cpus(self.cpus);
    }
}
