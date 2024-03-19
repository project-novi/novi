use crate::{Object, Result};
use once_cell::sync::Lazy;
use std::sync::RwLock;

pub trait HookPoint {
    const ID: usize;
}

macro_rules! define_hooks {
    ($($name:ty,)*) => {
        define_hooks!(@ 0, $($name,)*);
    };
    (@ $start:expr, $name:ty, $($rest:ty,)*) => {
        impl HookPoint for $name {
            const ID: usize = $start;
        }

        define_hooks!(@ $start + 1, $($rest,)*);
    };
    (@ $num:expr,) => {
        const NUM_TOTAL_HOOKS: usize = $num;
    };
}

pub struct BeforeAddObject<'a> {
    pub object: &'a mut Object,
}
pub struct AfterAddObject<'a> {
    pub object: &'a Object,
}
pub struct BeforeDeleteObject<'a> {
    pub object: &'a mut Object,
}
pub struct AfterDeleteObject<'a> {
    pub object: &'a Object,
}
pub struct BeforeUpdateObject<'a> {
    pub old: &'a Object,
    pub object: &'a mut Object,
}
pub struct AfterUpdateObject<'a> {
    pub old: &'a Object,
    pub object: &'a Object,
}

define_hooks!(
    BeforeAddObject<'_>,
    AfterAddObject<'_>,
    BeforeDeleteObject<'_>,
    AfterDeleteObject<'_>,
    BeforeUpdateObject<'_>,
    AfterUpdateObject<'_>,
);

type HookFn<T> = Box<dyn Fn(&mut T) -> Result<()> + Send + Sync>;
struct HookRegistry {
    hooks: [RwLock<Vec<HookFn<()>>>; NUM_TOTAL_HOOKS],
}
static REGISTRY: Lazy<HookRegistry> = Lazy::new(|| HookRegistry {
    hooks: Default::default(),
});

pub fn register<T: HookPoint>(f: HookFn<T>) {
    REGISTRY.hooks[T::ID]
        .write()
        .unwrap()
        .push(unsafe { std::mem::transmute(f) });
}

pub(crate) fn run<T: HookPoint>(mut data: T) -> Result<()> {
    for hook in REGISTRY.hooks[T::ID].read().unwrap().iter().rev() {
        let hook = unsafe { std::mem::transmute::<_, &HookFn<T>>(hook) };
        hook(&mut data)?;
    }
    Ok(())
}
