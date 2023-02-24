use my_no_sql_core::db::DbPartition;

use super::DbRowsSnapshot;

pub struct DbPartitionSnapshot {
    #[cfg(feature = "master-node")]
    pub last_read_moment: rust_extensions::date_time::DateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    pub last_write_moment: rust_extensions::date_time::DateTimeAsMicroseconds,
    pub db_rows_snapshot: DbRowsSnapshot,
}

#[cfg(feature = "master-node")]
impl DbPartitionSnapshot {
    pub fn has_to_persist(
        &self,
        written_in_blob: rust_extensions::date_time::DateTimeAsMicroseconds,
    ) -> bool {
        written_in_blob.unix_microseconds < self.last_write_moment.unix_microseconds
    }
}
/*
#[cfg(feature = "master-node")]
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

impl Into<DbRowsSnapshot> for &DbPartition {
    fn into(self) -> DbRowsSnapshot {
        DbRowsSnapshot::new_from_snapshot(self.rows.get_all().map(|itm| itm.clone()).collect())
    }
}

impl Into<DbPartitionSnapshot> for &DbPartition {
    fn into(self) -> DbPartitionSnapshot {
        DbPartitionSnapshot {
            #[cfg(feature = "master-node")]
            last_read_moment: self.last_read_moment.as_date_time(),
            #[cfg(feature = "master-node")]
            last_write_moment: self.last_write_moment.as_date_time(),
            db_rows_snapshot: self.into(),
        }
    }
}
