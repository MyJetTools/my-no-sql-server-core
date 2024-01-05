use std::collections::BTreeMap;

use my_json::json_writer::JsonArrayWriter;
use my_no_sql_sdk::core::db::DbTable;
#[cfg(feature = "master-node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::DbPartitionSnapshot;

pub struct DbTableSnapshot {
    #[cfg(feature = "master-node")]
    pub attr: my_no_sql_sdk::core::db::DbTableAttributes,
    #[cfg(feature = "master-node")]
    pub last_write_moment: DateTimeAsMicroseconds,
    pub by_partition: BTreeMap<String, DbPartitionSnapshot>,
}

impl DbTableSnapshot {
    pub fn new(
        #[cfg(feature = "master-node")] last_write_moment: DateTimeAsMicroseconds,
        db_table: &DbTable,
    ) -> Self {
        let mut by_partition = BTreeMap::new();

        for (partition_key, db_partition) in db_table.partitions.get_all() {
            by_partition.insert(partition_key.to_string(), db_partition.into());
        }

        Self {
            #[cfg(feature = "master-node")]
            attr: db_table.attributes.clone(),
            #[cfg(feature = "master-node")]
            last_write_moment,
            by_partition,
        }
    }

    pub fn as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();

        for db_partition_snapshot in self.by_partition.values() {
            for db_row in &db_partition_snapshot.db_rows_snapshot.db_rows {
                db_row.compile_json(json_array_writer.get_mut_to_write_raw_element());
            }
        }

        json_array_writer
    }
}
