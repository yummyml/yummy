use crate::delta::error::DeltaError;
use crate::delta::{DeltaInfo, DeltaManager};
use crate::models::{ResponseStore, ResponseTable, ResponseTables};
use async_trait::async_trait;
use deltalake::builder::DeltaTableBuilder;
use futures::future;
use futures::stream::StreamExt;
use itertools::Itertools;
use std::error::Error;

#[async_trait]
impl DeltaInfo for DeltaManager {
    fn list_stores(&self) -> Result<Vec<ResponseStore>, Box<dyn Error>> {
        let stores = self
            .config
            .stores
            .iter()
            .map(|s| ResponseStore {
                store: s.name.to_string(),
                path: s.path.to_string(),
            })
            .collect::<Vec<ResponseStore>>();

        Ok(stores)
    }

    async fn details(
        &self,
        store_name: &str,
        table_name: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<ResponseTable, Box<dyn Error>> {
        let table = self
            .table(store_name, table_name, table_version, table_date)
            .await?;
        let schema = table.schema().ok_or(Box::new(DeltaError::InvalidSchema))?;
        let version = table.version();
        let path = self.path(store_name, table_name)?;
        Ok(ResponseTable {
            store: store_name.to_string(),
            table: table_name.to_string(),
            path,
            schema: schema.clone(),
            version,
        })
    }

    async fn list_tables(&self, store_name: &str) -> Result<ResponseTables, Box<dyn Error>> {
        let store = &self.store(store_name)?;
        let uri = &store.path.to_string();
        let mut builder = DeltaTableBuilder::from_uri(uri).with_allow_http(true);

        if let Some(storage_options) = &store.storage_options {
            builder = builder.with_storage_options(storage_options.clone());
        }

        let st = builder.build_storage()?.storage_backend();

        let list_stream = st.list(None).await?;

        let tbl = list_stream
            .map(|meta| meta.expect("Error listing").location.to_string())
            .filter(|l| future::ready(l.contains("_delta_log")))
            .collect::<Vec<String>>()
            .await;

        let table_names = tbl
            .iter()
            .map(|l| l.split('/').collect::<Vec<&str>>()[0].to_string())
            .unique()
            .collect();

        Ok(ResponseTables {
            store: store.name.to_string(),
            path: store.path.to_string(),
            tables: table_names,
        })
    }
}
