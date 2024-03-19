use crate::{Filter, Object};
use std::{
    collections::{BTreeSet, HashMap},
    hash::Hash,
};

/// Dispatch event efficiently by filters.
/// Currently the approach is naive, but it can be optimized later.
pub struct Dispatcher<K, V> {
    receivers: HashMap<K, (Filter, V)>,
}
impl<K: Eq + Hash, V> Dispatcher<K, V> {
    pub fn new() -> Self {
        Dispatcher {
            receivers: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, filter: Filter, value: V) {
        self.receivers.insert(key, (filter, value));
    }

    pub fn dispatch<'a, 'b>(
        &'a self,
        object: &'b Object,
        exclude: impl Fn(&V) -> bool + 'a,
        deleted_tags: &'a BTreeSet<String>,
    ) -> impl Iterator<Item = (&'a K, &'a Filter, &'a V)> + 'b
    where
        'a: 'b,
    {
        self.receivers
            .iter()
            .filter(move |(_, (f, v))| f.satisfies_excluding(object, exclude(v), deleted_tags))
            .map(|(k, (f, v))| (k, f as &Filter, v))
    }

    pub fn dispatch_mut<'a, 'b>(
        &'a mut self,
        object: &'b Object,
        exclude: impl Fn(&V) -> bool + 'a,
        deleted_tags: &'a BTreeSet<String>,
    ) -> impl Iterator<Item = (&'a K, &'a Filter, &'a mut V)> + 'b
    where
        'a: 'b,
    {
        self.receivers
            .iter_mut()
            .filter(move |(_, (f, v))| f.satisfies_excluding(object, exclude(v), deleted_tags))
            .map(|(k, (f, v))| (k, f as &Filter, v))
    }

    pub fn remove(&mut self, key: &K) {
        self.receivers.remove(key);
    }
}

impl<K: Eq + Hash, V> Default for Dispatcher<K, V> {
    fn default() -> Self {
        Dispatcher::new()
    }
}

impl<K: Eq + Hash, V> FromIterator<(K, Filter, V)> for Dispatcher<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, Filter, V)>>(iter: I) -> Self {
        Dispatcher {
            receivers: iter.into_iter().map(|(k, f, v)| (k, (f, v))).collect(),
        }
    }
}
