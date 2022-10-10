use super::{
    BaseRocksSecondaryIndex, CacheItem, IndexId, RocksSecondaryIndex, RocksTable, TableId,
};
use crate::metastore::{ColumnFamilyName, IdRow, MetaStoreEvent};
use crate::rocks_table_impl;

use crate::base_rocks_secondary_index;

use chrono::{DateTime, Duration, Utc};

use rocksdb::DB;
use serde::{Deserialize, Deserializer};
use std::ops::Add;

impl CacheItem {
    pub fn new(path: String, ttl: Option<u32>, value: String) -> CacheItem {
        let parts: Vec<&str> = path.rsplitn(2, ":").collect();

        let (prefix, key) = match parts.len() {
            2 => (parts[1].to_string(), parts[0].to_string()),
            _ => ("".to_string(), path),
        };

        CacheItem {
            prefix,
            key,
            value,
            expire: ttl.map(|ttl| Utc::now().add(Duration::seconds(ttl as i64))),
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expire) = self.get_expire() {
            if expire < &Utc::now() {
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn get_path(&self) -> String {
        if self.prefix == "" {
            self.key.clone()
        } else {
            format!("{}:{}", self.prefix, self.key)
        }
    }

    pub fn get_prefix(&self) -> &String {
        &self.prefix
    }

    pub fn get_key(&self) -> &String {
        &self.key
    }

    pub fn get_expire(&self) -> &Option<DateTime<Utc>> {
        &self.expire
    }

    pub fn get_value(&self) -> &String {
        &self.value
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum CacheItemRocksIndex {
    ByPath = 1,
    ByPrefix = 2,
}

rocks_table_impl!(
    CacheItem,
    CacheItemRocksTable,
    TableId::CacheItems,
    {
        vec![
            Box::new(CacheItemRocksIndex::ByPath),
            Box::new(CacheItemRocksIndex::ByPrefix),
        ]
    },
    ColumnFamilyName::Cache
);

#[derive(Hash, Clone, Debug)]
pub enum CacheItemIndexKey {
    // prefix + key
    ByPath(String),
    ByPrefix(String),
}

base_rocks_secondary_index!(CacheItem, CacheItemRocksIndex);

impl RocksSecondaryIndex<CacheItem, CacheItemIndexKey> for CacheItemRocksIndex {
    fn typed_key_by(&self, row: &CacheItem) -> CacheItemIndexKey {
        match self {
            CacheItemRocksIndex::ByPath => CacheItemIndexKey::ByPath(row.get_path()),
            CacheItemRocksIndex::ByPrefix => CacheItemIndexKey::ByPrefix(row.prefix.clone()),
        }
    }

    fn key_to_bytes(&self, key: &CacheItemIndexKey) -> Vec<u8> {
        match key {
            CacheItemIndexKey::ByPath(path) => path.as_bytes().to_vec(),
            CacheItemIndexKey::ByPrefix(prefix) => prefix.as_bytes().to_vec(),
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            CacheItemRocksIndex::ByPath => true,
            CacheItemRocksIndex::ByPrefix => false,
        }
    }

    fn version(&self) -> u32 {
        match self {
            CacheItemRocksIndex::ByPath => 1,
            CacheItemRocksIndex::ByPrefix => 1,
        }
    }

    fn is_ttl(&self) -> bool {
        true
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
