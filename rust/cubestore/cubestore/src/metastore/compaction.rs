use crate::metastore::RowKey;
use chrono::{DateTime, Utc};
use log::error;
use rocksdb::compaction_filter::CompactionFilter;
use rocksdb::compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory};
use rocksdb::CompactionDecision;
use std::ffi::{CStr, CString};

pub struct MetaStoreCacheCompactionFilter {
    name: CString,
    current: DateTime<Utc>,
    removed: u64,
    orphaned: u64,
}

impl MetaStoreCacheCompactionFilter {
    pub fn new() -> Self {
        Self {
            name: CString::new("cache-expire-check").unwrap(),
            current: Utc::now(),
            removed: 0,
            orphaned: 0,
        }
    }
}

impl CompactionFilter for MetaStoreCacheCompactionFilter {
    fn filter(&mut self, level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        println!(
            "meta_store_cache_cf_compaction level {} key {:?} value {:?}",
            level, key, value
        );

        if let Ok(row_key) = RowKey::try_from_bytes(key) {
            println!("key {:?} - value {:?}", row_key, value);

            return match row_key {
                RowKey::Table(table_id, _) => {
                    if !table_id.has_ttl() {
                        return CompactionDecision::Keep;
                    }

                    if let Ok(reader) = flexbuffers::Reader::get_root(&value) {
                        let root = reader.as_map();

                        if let Some(expire_key_id) = root.index_key(&"expire") {
                            let res = chrono::DateTime::parse_from_rfc3339(
                                root.idx(expire_key_id).as_str(),
                            );
                            match res {
                                Ok(expire) => {
                                    if expire <= self.current {
                                        self.removed += 1;

                                        return CompactionDecision::Remove;
                                    }
                                }
                                Err(err) => {
                                    error!("While compaction: {}", err);

                                    self.orphaned += 1;

                                    return CompactionDecision::Remove;
                                }
                            }
                        }
                    }

                    CompactionDecision::Keep
                }
                RowKey::Sequence(_) => CompactionDecision::Keep,
                RowKey::SecondaryIndex(_index_id, _secondary_key, _row_id) => {
                    CompactionDecision::Keep
                }
                RowKey::SecondaryIndexInfo { .. } => CompactionDecision::Keep,
            };
        } else {
            error!("Unable to read key on metastore cache compaction");

            CompactionDecision::Keep
        }
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

pub struct MetaStoreCacheCompactionFactory(CString);

impl MetaStoreCacheCompactionFactory {
    pub fn new() -> Self {
        Self(CString::new("cache-expire-check").unwrap())
    }
}

impl CompactionFilterFactory for MetaStoreCacheCompactionFactory {
    type Filter = MetaStoreCacheCompactionFilter;

    fn create(&mut self, _: CompactionFilterContext) -> Self::Filter {
        MetaStoreCacheCompactionFilter::new()
    }

    fn name(&self) -> &CStr {
        &self.0
    }
}
