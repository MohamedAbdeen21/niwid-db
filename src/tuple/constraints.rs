use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Constraints {
    pub nullable: bool,
    pub unique: bool,
}

impl Default for Constraints {
    fn default() -> Self {
        Self {
            nullable: true,
            unique: false,
        }
    }
}

impl Constraints {
    pub fn new(nullable: bool, unique: bool) -> Self {
        Self { nullable, unique }
    }

    pub fn nullable(value: bool) -> Constraints {
        Constraints {
            nullable: value,
            ..Default::default()
        }
    }

    pub fn unique(value: bool) -> Constraints {
        // TODO:
        // unqiue columns can't have nulls
        // as this impl of b+tree doesn't support null keys
        if value {
            Constraints::new(false, true)
        } else {
            Constraints::new(true, false)
        }
    }
}
