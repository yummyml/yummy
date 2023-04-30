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

#[cfg(test)]
mod test {
    use crate::delta::test_delta_util::{create_delta, create_manager, drop_delta};
    use crate::delta::DeltaInfo;
    use std::error::Error;
    use std::fs;

    #[tokio::test]
    async fn test_delta_list_stores() -> Result<(), Box<dyn Error>> {
        let stores = create_manager().await?.list_stores()?;

        assert_eq!(stores.len(), 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_list_tables_local() -> Result<(), Box<dyn Error>> {
        let store_name = String::from("local2");
        let table_name1 = String::from("test_delta_1");
        let table_name2 = String::from("test_delta_2");

        let _table1 = create_delta(&store_name, &table_name1).await?;
        let _table2 = create_delta(&store_name, &table_name2).await?;

        let tables = create_manager().await?.list_tables(&store_name).await?;
        println!("{:?}", tables.tables);
        assert_eq!(tables.tables.len(), 2);
        fs::remove_dir_all("/tmp/delta-test-2")?;
        Ok(())
    }

    #[tokio::test]
    async fn test_delta_details() -> Result<(), Box<dyn Error>> {
        let store_name = String::from("local");
        let table_name = String::from("test_delta");

        let table = create_delta(&store_name, &table_name).await?;

        let resp = create_manager()
            .await?
            .details(&store_name, &table_name, None, None)
            .await?;
        assert_eq!(table.version(), 0);
        assert_eq!(resp.version, 0);

        drop_delta(&table_name).await?;
        Ok(())
    }
}
