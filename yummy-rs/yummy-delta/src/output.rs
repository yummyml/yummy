use crate::models::{ResponseStore, ResponseTable, ResponseTables};
use async_trait::async_trait;
use deltalake::schema::SchemaDataType;
use prettytable::{row, Table};
use yummy_core::common::Result;

#[async_trait]
pub trait PrettyOutput {
    fn to_prettytable(&self) -> Result<String>;
}

impl PrettyOutput for Vec<ResponseStore> {
    fn to_prettytable(&self) -> Result<String> {
        //println!("{:#?}", &df.schema());
        let mut tbl = Table::new();

        tbl.set_titles(row!["store", "path"]);
        for s in self.iter() {
            tbl.add_row(row![&s.store, &s.path,]);
        }

        Ok(tbl.to_string())
    }
}

impl PrettyOutput for ResponseTables {
    fn to_prettytable(&self) -> Result<String> {
        //println!("{:#?}", &df.schema());
        let mut tbl = Table::new();

        tbl.set_titles(row!["table"]);
        for t in self.tables.iter() {
            tbl.add_row(row![&t]);
        }

        Ok(tbl.to_string())
    }
}

impl PrettyOutput for ResponseTable {
    fn to_prettytable(&self) -> Result<String> {
        //println!("{:#?}", &df.schema());
        let mut tbl = Table::new();

        tbl.set_titles(row!["name", "type", "nullable", "metadata"]);
        for f in self.schema.get_fields() {
            let f_str = match f.get_type() {
                SchemaDataType::primitive(p) => p.to_string(),
                SchemaDataType::array(a) => format!("{a:#?}"),
                SchemaDataType::map(m) => format!("{m:#?}"),
                SchemaDataType::r#struct(s) => format!("{s:#?}"),
            };

            tbl.add_row(row![
                f.get_name(),
                f_str,
                f.is_nullable(),
                format!("{:#?}", f.get_metadata())
            ]);
        }

        Ok(tbl.to_string())
    }
}

#[tokio::test]
async fn test_prettytable_table() -> Result<()> {
    let schema: deltalake::Schema = serde_json::from_value(serde_json::json!({
            "type": "struct",
            "fields": [{
                "name": "a_map",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": {
                        "type": "array",
                        "elementType": {
                            "type": "struct",
                            "fields": [{
                                "name": "d",
                                "type": "integer",
                                "metadata": {
                                    "delta.invariants": "{\"expression\": { \"expression\": \"a_map.value.element.d < 4\"} }"
                                },
                                "nullable": false
                            }]
                        },
                        "containsNull": false
                    },
                    "valueContainsNull": false
                },
                "nullable": false,
                "metadata": {}
            }]
        })).unwrap();

    let table = ResponseTable {
        path: "path".to_string(),
        store: "store".to_string(),
        table: "table".to_string(),
        version: 1,
        schema,
    };

    let output = table.to_prettytable()?;
    println!("{output}");

    Ok(())
}

#[tokio::test]
async fn test_prettytable_stores() -> Result<()> {
    let stores: Vec<ResponseStore> = vec![
        ResponseStore {
            store: "name1".to_string(),
            path: "path1".to_string(),
        },
        ResponseStore {
            store: "name2".to_string(),
            path: "path2".to_string(),
        },
    ];

    let output = stores.to_prettytable()?;
    println!("{output}");

    Ok(())
}

#[tokio::test]
async fn test_prettytable_tables() -> Result<()> {
    let tables = ResponseTables {
        path: "path".to_string(),
        store: "store".to_string(),
        tables: vec!["table1".to_string(), "table2".to_string()],
    };

    let output = tables.to_prettytable()?;

    println!("{output}");

    Ok(())
}
