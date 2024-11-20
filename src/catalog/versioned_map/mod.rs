use std::collections::HashMap;

#[derive(Default)]
pub struct VersionedMap<K, V> {
    /// Base version of the data
    base: HashMap<K, V>,
    /// Versioned changes, set to None if version deleted entry
    versions: HashMap<u64, HashMap<K, Option<V>>>,
}

impl<K: std::cmp::Eq + std::hash::Hash + Clone, V> VersionedMap<K, V> {
    pub fn new() -> Self {
        VersionedMap {
            base: HashMap::new(),
            versions: HashMap::new(),
        }
    }

    /// Insert or modify a table in a specific version
    pub fn insert(&mut self, version: Option<u64>, key: K, value: V) {
        if let Some(version) = version {
            self.versions
                .entry(version)
                .or_default()
                .insert(key, Some(value));
        } else {
            self.base.insert(key.clone(), value);
        }
    }

    pub fn get(&self, version: Option<u64>, key: &K) -> Option<&V> {
        if let Some(version) = version {
            if let Some(changes) = self.versions.get(&version) {
                if let Some(entry) = changes.get(key) {
                    if let Some(table) = entry {
                        return Some(table);
                    } else {
                        return None;
                    }
                }
            }
        }

        self.base.get(key)
    }

    /// Get mutable reference to a table in a specific version, cloning if necessary
    pub fn get_mut(&mut self, version: Option<u64>, key: &K) -> Option<&mut V> {
        let changes = if let Some(version) = version {
            self.versions.entry(version).or_default()
        } else {
            return self.base.get_mut(key);
        };

        match changes.get_mut(key) {
            Some(Some(table)) => Some(table),
            Some(None) => None,
            None => self.base.get_mut(key),
        }
    }

    pub fn remove(&mut self, version: Option<u64>, key: &K) -> Option<V> {
        if let Some(version) = version {
            let changes = self.versions.entry(version).or_default();
            changes.insert(key.clone(), None);
            None // Indicate that the value has been marked for deletion in the version
        } else {
            self.base.remove(key)
        }
    }

    /// Commit a version: apply all changes to the base map
    pub fn commit(&mut self, version: u64) -> Vec<K> {
        let mut refs = vec![];

        if let Some(changes) = self.versions.remove(&version) {
            for (key, value) in changes {
                match value {
                    Some(v) => {
                        refs.push(key.clone());
                        self.base.insert(key, v) // Apply insert/update
                    }
                    None => self.base.remove(&key), // Apply deletion
                };
            }
        };

        refs
    }

    /// rollback a version: discard all changes without applying them
    pub fn rollback(&mut self, version: u64) {
        self.versions.remove(&version);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_commit() -> Result<()> {
        let mut map = VersionedMap::new();
        map.insert(None, "table1".to_string(), 42);
        map.insert(Some(1), "table1".to_string(), 100);
        assert_eq!(map.get(None, &"table1".to_string()), Some(&42));
        assert_eq!(map.get(Some(1), &"table1".to_string()), Some(&100));
        map.commit(1);
        assert_eq!(map.get(None, &"table1".to_string()), Some(&100));
        Ok(())
    }

    #[test]
    fn test_rollback() -> Result<()> {
        let mut map = VersionedMap::new();
        map.insert(None, "table1".to_string(), 42);
        map.insert(Some(1), "table1".to_string(), 100);
        map.rollback(1);
        assert_eq!(map.get(None, &"table1".to_string()), Some(&42));
        Ok(())
    }

    #[test]
    fn test_remove() -> Result<()> {
        let mut map = VersionedMap::new();
        map.insert(None, "table1".to_string(), 42);
        map.remove(None, &"table1".to_string());
        assert_eq!(map.get(None, &"table1".to_string()), None);

        map.insert(None, "table1".to_string(), 42);
        map.insert(Some(1), "table1".to_string(), 100);
        map.remove(Some(1), &"table1".to_string());
        assert_eq!(map.get(Some(1), &"table1".to_string()), None);
        assert_eq!(map.get(None, &"table1".to_string()), Some(&42));
        assert_eq!(map.remove(Some(2), &"table1".to_string()), None);
        Ok(())
    }
}
