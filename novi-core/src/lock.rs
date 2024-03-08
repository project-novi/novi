use dashmap::DashMap;
use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque},
    hash::Hash,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

pub trait Empty {
    fn is_empty(&self) -> bool;
}

impl Empty for () {
    fn is_empty(&self) -> bool {
        true
    }
}
impl<V> Empty for Option<V> {
    fn is_empty(&self) -> bool {
        Option::is_none(self)
    }
}
impl<V> Empty for Vec<V> {
    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }
}
impl<V> Empty for VecDeque<V> {
    fn is_empty(&self) -> bool {
        VecDeque::is_empty(self)
    }
}
impl<V> Empty for LinkedList<V> {
    fn is_empty(&self) -> bool {
        LinkedList::is_empty(self)
    }
}
impl<V> Empty for BinaryHeap<V> {
    fn is_empty(&self) -> bool {
        BinaryHeap::is_empty(self)
    }
}
impl<K, V> Empty for BTreeMap<K, V> {
    fn is_empty(&self) -> bool {
        BTreeMap::is_empty(self)
    }
}
impl<V> Empty for BTreeSet<V> {
    fn is_empty(&self) -> bool {
        BTreeSet::is_empty(self)
    }
}
impl<K, V> Empty for HashMap<K, V> {
    fn is_empty(&self) -> bool {
        HashMap::is_empty(self)
    }
}
impl<V> Empty for HashSet<V> {
    fn is_empty(&self) -> bool {
        HashSet::is_empty(self)
    }
}
impl<K: Eq + Hash + Clone, V: Empty + Default> Empty for KeyMutex<K, V> {
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl<K: Eq + Hash + Clone, V: Empty + Default> Empty for KeyRwLock<K, V> {
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

type KeyLockInner<K, L> = Arc<DashMap<K, Arc<L>>>;

struct KeyRef<K, L> {
    map: KeyLockInner<K, L>,
    key: K,
}
impl<K: Eq + Hash, L> KeyRef<K, L> {
    fn remove_if_needed<V: Empty, G: Deref<Target = V>>(&self, guard: &mut ManuallyDrop<G>) {
        if guard.is_empty() {
            let guard = unsafe { ManuallyDrop::take(guard) };
            self.map.remove_if(&self.key, |_, v| {
                std::mem::drop(guard);
                Arc::strong_count(v) == 1
            });
        } else {
            unsafe { ManuallyDrop::drop(guard) };
        }
    }
}

pub struct OwnedReadGuard<K: Eq + Hash, V: Empty> {
    key: KeyRef<K, RwLock<V>>,
    guard: ManuallyDrop<OwnedRwLockReadGuard<V>>,
}
impl<K: Eq + Hash, V: Empty> OwnedReadGuard<K, V> {
    pub fn key(slot: &Self) -> &K {
        &slot.key.key
    }
}
impl<K: Eq + Hash, V: Empty> Deref for OwnedReadGuard<K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}
impl<K: Eq + Hash, V: Empty> Drop for OwnedReadGuard<K, V> {
    fn drop(&mut self) {
        self.key.remove_if_needed(&mut self.guard);
    }
}

pub struct OwnedWriteGuard<K: Eq + Hash, V: Empty> {
    key: KeyRef<K, RwLock<V>>,
    guard: ManuallyDrop<OwnedRwLockWriteGuard<V>>,
}
impl<K: Eq + Hash, V: Empty> OwnedWriteGuard<K, V> {
    pub fn key(slot: &Self) -> &K {
        &slot.key.key
    }
}
impl<K: Eq + Hash, V: Empty> Deref for OwnedWriteGuard<K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<K: Eq + Hash, V: Empty> DerefMut for OwnedWriteGuard<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

impl<K: Eq + Hash, V: Empty> Drop for OwnedWriteGuard<K, V> {
    fn drop(&mut self) {
        self.key.remove_if_needed(&mut self.guard);
    }
}

#[derive(Clone, Default)]
pub struct KeyRwLock<K: Eq + Hash, V>(KeyLockInner<K, RwLock<V>>);
impl<K: Eq + Hash + Clone, V: Empty + Default> KeyRwLock<K, V> {
    pub fn new() -> Self {
        Self(Arc::default())
    }

    fn key_ref(&self, key: K) -> KeyRef<K, RwLock<V>> {
        KeyRef {
            map: self.0.clone(),
            key,
        }
    }

    pub async fn read(&self, key: K) -> OwnedReadGuard<K, V> {
        OwnedReadGuard {
            key: self.key_ref(key.clone()),
            guard: ManuallyDrop::new({
                let lock = self.0.entry(key).or_insert_with(Arc::default).clone();
                lock.read_owned().await
            }),
        }
    }

    pub async fn write(&self, key: K) -> OwnedWriteGuard<K, V> {
        OwnedWriteGuard {
            key: self.key_ref(key.clone()),
            guard: ManuallyDrop::new({
                let lock = self.0.entry(key).or_insert_with(Arc::default).clone();
                lock.write_owned().await
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct OwnedMutexGuard<K: Eq + Hash, V: Empty> {
    key: KeyRef<K, Mutex<V>>,
    guard: ManuallyDrop<tokio::sync::OwnedMutexGuard<V>>,
}
impl<K: Eq + Hash, V: Empty> OwnedMutexGuard<K, V> {
    pub fn key(slot: &Self) -> &K {
        &slot.key.key
    }
}
impl<K: Eq + Hash, V: Empty> Deref for OwnedMutexGuard<K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}
impl<K: Eq + Hash, V: Empty> DerefMut for OwnedMutexGuard<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}
impl<K: Eq + Hash, V: Empty> Drop for OwnedMutexGuard<K, V> {
    fn drop(&mut self) {
        self.key.remove_if_needed(&mut self.guard);
    }
}
#[derive(Clone, Default)]
pub struct KeyMutex<K: Eq + Hash, V>(KeyLockInner<K, Mutex<V>>);
impl<K: Eq + Hash + Clone, V: Empty + Default> KeyMutex<K, V> {
    pub fn new() -> Self {
        Self(Arc::default())
    }

    pub async fn lock(&self, key: K) -> OwnedMutexGuard<K, V> {
        OwnedMutexGuard {
            key: KeyRef {
                map: self.0.clone(),
                key: key.clone(),
            },
            guard: ManuallyDrop::new({
                let lock = self.0.entry(key).or_insert_with(Arc::default).clone();
                lock.lock_owned().await
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod test {
    use super::KeyMutex;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn drop_only_if_empty() {
        let locks = KeyMutex::<u32, BTreeSet<String>>::new();

        let mut lock = locks.lock(1).await;
        lock.insert("Hello".to_owned());
        lock.insert("World".to_owned());
        drop(lock);

        // Value is not empty and thus is not dropped
        assert_eq!(locks.len(), 1);

        let mut lock = locks.lock(1).await;
        assert_eq!(lock.len(), 2);
        lock.clear();
        drop(lock);

        // Should be dropped now
        assert_eq!(locks.len(), 0);
    }
}
