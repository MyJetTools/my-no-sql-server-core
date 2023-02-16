use std::sync::atomic::AtomicUsize;

use my_no_sql_core::db::DbTable;

pub struct DbTableServerWrapper {
    pub data: DbTable,
    max_partitions_amount: AtomicUsize,
}

impl DbTableServerWrapper {
    pub fn new(data: DbTable, max_partitions_amount: AtomicUsize) -> DbTableServerWrapper {
        DbTableServerWrapper {
            data,
            max_partitions_amount,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.data.name
    }

    pub fn get_max_partitions_amount(&self) -> Option<usize> {
        let result = self
            .max_partitions_amount
            .load(std::sync::atomic::Ordering::Relaxed);

        if result == 0 {
            return None;
        }

        return Some(result);
    }
}
