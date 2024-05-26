use uuid::Uuid;

use crate::tag::Tags;

pub trait Model {
    fn id(&self) -> Uuid;
    fn to_tags(&self) -> Tags;
}
