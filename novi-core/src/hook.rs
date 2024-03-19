use crate::{dispatch::Dispatcher, Filter, Object, Result};
use once_cell::sync::Lazy;
use std::{collections::BTreeSet, sync::RwLock};

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
#[derive(Default)]
struct HookPointState {
    hooks: Dispatcher<u64, HookFn<()>>,
    gen: u32,
}
impl HookPointState {
    fn insert(&mut self, f: HookFn<()>, filter: Filter, priority: u32) -> u64 {
        let id = self.gen as u64 | ((priority as u64) << 32);
        self.gen += 1;
        self.hooks.insert(id, filter, f);
        id
    }

    fn run<T>(
        &self,
        mut data: T,
        object: impl for<'a> FnOnce(&'a T) -> &'a Object,
        deleted_tags: &BTreeSet<String>,
    ) -> Result<()> {
        let hooks = self
            .hooks
            .dispatch(object(&data), |_| false, deleted_tags)
            .map(|(_, _, f)| f)
            .collect::<Vec<_>>();

        for hook in hooks {
            let hook: &HookFn<T> = unsafe { std::mem::transmute(hook) };
            hook(&mut data)?;
        }
        Ok(())
    }
}

struct HookRegistry {
    hooks: [RwLock<HookPointState>; NUM_TOTAL_HOOKS],
}
static REGISTRY: Lazy<HookRegistry> = Lazy::new(|| HookRegistry {
    hooks: Default::default(),
});

pub fn register<T: HookPoint>(f: HookFn<T>, filter: Filter, priority: u32) -> u64 {
    REGISTRY.hooks[T::ID].write().unwrap().insert(
        unsafe { std::mem::transmute(f) },
        filter,
        priority,
    )
}

pub(crate) fn run<T: HookPoint>(
    data: T,
    object: impl FnOnce(&T) -> &Object,
    deleted_tags: &BTreeSet<String>,
) -> Result<()> {
    REGISTRY.hooks[T::ID]
        .read()
        .unwrap()
        .run(data, object, deleted_tags)
}
