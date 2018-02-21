#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    pub cpus: u32,
}

impl Resources {

    #[inline]
    pub fn cpus(&self) -> u32 {
        self.cpus
    }

    pub fn add(&mut self, resources: &Resources) {
        self.cpus += resources.cpus;
    }

    pub fn remove(&mut self, resources: &Resources) {
        assert!(self.cpus >= resources.cpus);
        self.cpus -= resources.cpus;
    }

    pub fn difference(&self, resources: &Resources) -> Resources {
        assert!(self.cpus >= resources.cpus);
        Resources {
            cpus: self.cpus - resources.cpus,
        }
    }

    pub fn from_capnp(reader: &::common_capnp::resources::Reader) -> Self {
        Resources { cpus: reader.get_n_cpus() }
    }

    pub fn to_capnp(&self, builder: &mut ::common_capnp::resources::Builder) {
        builder.set_n_cpus(self.cpus);
    }

    #[inline]
    pub fn is_subset_of(&self, resources: &Resources) -> bool {
        self.cpus <= resources.cpus
    }
}
