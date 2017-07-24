
type Id = i32;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Sid {
    session_id: Id,
    id: Id,
}

impl Sid {
    pub fn new(session_id: Id, id: Id) -> Self {
        Self {
            session_id: session_id,
            id: id,
        }
    }

    #[inline]
    pub fn get_id(&self) -> Id {
        self.id
    }

    #[inline]
    pub fn get_session_id(&self) -> Id {
        self.session_id
    }
}
