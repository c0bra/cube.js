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
    pub fn new(key: String, ttl: Option<u32>, value: String) -> CacheItem {
        CacheItem {
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
    Key = 1,
}

rocks_table_impl!(
    CacheItem,
    CacheItemRocksTable,
    TableId::CacheItems,
    { vec![Box::new(CacheItemRocksIndex::Key)] },
    ColumnFamilyName::Cache
);

#[derive(Hash, Clone, Debug)]
pub enum CacheItemIndexKey {
    ByKey(String),
}

base_rocks_secondary_index!(CacheItem, CacheItemRocksIndex);

impl RocksSecondaryIndex<CacheItem, CacheItemIndexKey> for CacheItemRocksIndex {
    fn typed_key_by(&self, row: &CacheItem) -> CacheItemIndexKey {
        match self {
            CacheItemRocksIndex::Key => CacheItemIndexKey::ByKey(row.key.clone()),
        }
    }

    fn key_to_bytes(&self, key: &CacheItemIndexKey) -> Vec<u8> {
        match key {
            CacheItemIndexKey::ByKey(key) => key.as_bytes().to_vec(),
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            CacheItemRocksIndex::Key => true,
        }
    }

    fn version(&self) -> u32 {
        match self {
            CacheItemRocksIndex::Key => 1,
        }
    }

    fn is_ttl(&self) -> bool {
        match self {
            CacheItemRocksIndex::Key => true,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
