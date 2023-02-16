#[cfg(feature = "master_node")]
use my_no_sql_core::db::DbPartition;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::DbRowsSnapshot;

pub struct DbPartitionSnapshot {
    pub last_read_moment: DateTimeAsMicroseconds,

    pub last_write_moment: DateTimeAsMicroseconds,
    pub db_rows_snapshot: DbRowsSnapshot,
}

impl DbPartitionSnapshot {
    pub fn has_to_persist(&self, written_in_blob: DateTimeAsMicroseconds) -> bool {
        written_in_blob.unix_microseconds < self.last_write_moment.unix_microseconds
    }
}
/*
#[cfg(feature = "master_node")]
impl Into<BTreeMap<String, DbPartitionSnapshot>> for &DbTable {
    fn into(self) -> BTreeMap<String, DbPartitionSnapshot> {
        let mut result: BTreeMap<String, DbPartitionSnapshot> = BTreeMap::new();

        for (partition_key, db_partition) in &self.partitions {
            result.insert(partition_key.to_string(), db_partition.into());
        }

        result
    }
}
 */
#[cfg(feature = "master_node")]
impl Into<DbRowsSnapshot> for &DbPartition {
    fn into(self) -> DbRowsSnapshot {
        DbRowsSnapshot::new_from_snapshot(self.rows.get_all().map(|itm| itm.clone()).collect())
    }
}
#[cfg(feature = "master_node")]
impl Into<DbPartitionSnapshot> for &DbPartition {
    fn into(self) -> DbPartitionSnapshot {
        DbPartitionSnapshot {
            last_read_moment: self.last_read_moment.as_date_time(),
            last_write_moment: self.last_write_moment.as_date_time(),
            db_rows_snapshot: self.into(),
        }
    }
}
