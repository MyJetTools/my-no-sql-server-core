use std::{collections::BTreeMap, sync::Arc};

use my_json::json_writer::JsonArrayWriter;
use my_no_sql_sdk::core::db::DbRow;

pub struct DbRowsByPartitionsSnapshot {
    pub partitions: BTreeMap<String, Vec<Arc<DbRow>>>,
}

impl DbRowsByPartitionsSnapshot {
    pub fn new() -> Self {
        Self {
            partitions: BTreeMap::new(),
        }
    }

    pub fn has_elements(&self) -> bool {
        self.partitions.len() > 0
    }

    pub fn add_row(&mut self, db_row: Arc<DbRow>) {
        let partition_key = db_row.get_partition_key();
        if !self.partitions.contains_key(partition_key) {
            self.partitions
                .insert(partition_key.to_string(), Vec::new());
        }

        self.partitions.get_mut(partition_key).unwrap().push(db_row);
    }

    pub fn add_rows(&mut self, partition_key: &str, db_rows: Vec<Arc<DbRow>>) {
        if !self.partitions.contains_key(partition_key) {
            self.partitions.insert(partition_key.to_string(), db_rows);
            return;
        }

        self.partitions
            .get_mut(partition_key)
            .unwrap()
            .extend(db_rows);
    }

    pub fn as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();
        for snapshot in self.partitions.values() {
            for db_row in snapshot {
                db_row.compile_json(json_array_writer.get_mut_to_write_raw_element());
            }
        }

        json_array_writer
    }
}
