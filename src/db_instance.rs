use tokio::sync::RwLock;

use std::{collections::HashMap, sync::Arc};

use crate::DbTableServerWrapper;

pub struct DbInstance {
    pub tables: RwLock<HashMap<String, Arc<DbTableServerWrapper>>>,
}

impl DbInstance {
    pub fn new() -> DbInstance {
        DbInstance {
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_table_names(&self) -> Vec<String> {
        let read_access = self.tables.read().await;

        return read_access
            .values()
            .into_iter()
            .map(|table| table.get_name().to_string())
            .collect();
    }

    pub async fn get_tables(&self) -> Vec<Arc<DbTableServerWrapper>> {
        let read_access = self.tables.read().await;

        return read_access
            .values()
            .into_iter()
            .map(|table| table.clone())
            .collect();
    }

    pub async fn get_table(&self, table_name: &str) -> Option<Arc<DbTableServerWrapper>> {
        let read_access = self.tables.read().await;

        let result = read_access.get(table_name)?;
        return Some(result.clone());
    }

    pub async fn delete_table(&self, table_name: &str) -> Option<Arc<DbTableServerWrapper>> {
        let mut write_access = self.tables.write().await;
        return write_access.remove(table_name);
    }
}
