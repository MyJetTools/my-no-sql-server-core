use std::{collections::VecDeque, sync::Arc};

use crate::db_snapshots::{DbPartitionSnapshot, DbTableSnapshot};
use my_json::json_writer::JsonArrayWriter;
use my_no_sql_core::db::{DbRow, DbTable};
use tokio::sync::RwLock;

pub struct DbTableWrapper {
    pub name: String,
    pub data: RwLock<DbTable>,
}

impl DbTableWrapper {
    pub fn new(db_table: DbTable) -> Arc<Self> {
        let result = Self {
            name: db_table.name.clone(),
            data: RwLock::new(db_table),
        };

        Arc::new(result)
    }

    pub async fn get_table_as_json_array(&self) -> JsonArrayWriter {
        let read_access = self.data.read().await;
        read_access.get_table_as_json_array()
    }

    pub async fn get_all_as_vec_dequeue(&self) -> VecDeque<Arc<DbRow>> {
        let read_access = self.data.read().await;

        let mut result = VecDeque::new();

        for db_row in read_access.get_all_rows() {
            result.push_back(db_row.clone());
        }

        result
    }

    pub async fn get_table_snapshot(&self) -> DbTableSnapshot {
        let read_access = self.data.read().await;

        DbTableSnapshot {
            last_update_time: read_access.get_last_update_time(),
            by_partition: get_partitions_snapshot(&read_access),
            #[cfg(feature = "master_node")]
            attr: read_access.attributes.clone(),
        }
    }

    pub async fn get_partitions_amount(&self) -> usize {
        let read_access = self.data.read().await;
        read_access.partitions.len()
    }

    pub async fn get_table_size(&self) -> usize {
        let read_access = self.data.read().await;
        read_access.get_table_size()
    }
}

fn get_partitions_snapshot(
    db_table: &DbTable,
) -> std::collections::BTreeMap<String, DbPartitionSnapshot> {
    let mut result = std::collections::BTreeMap::new();

    for (partition_key, db_partition) in &db_table.partitions {
        result.insert(partition_key.to_string(), db_partition.into());
    }

    result
}
