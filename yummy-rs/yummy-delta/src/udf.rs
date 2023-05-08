use crate::delta::read::map_array;
use crate::models::UdfConfig;
use async_trait::async_trait;
use datafusion::arrow::{
    array::{ArrayRef, Float64Array},
    datatypes::DataType,
};
use datafusion::physical_plan::{functions::make_scalar_function, udf::ScalarUDF};
use datafusion_expr::create_udf;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;
use yummy_core::common::Result;

static UDF_CONFIGS: OnceCell<HashMap<String, UdfConfig>> = OnceCell::new();

#[derive(Debug)]
pub struct UdfBuilder {}

impl UdfBuilder {
    pub fn init(udf_configs: HashMap<String, UdfConfig>) {
        UDF_CONFIGS.set(udf_configs).unwrap();
    }

    pub fn get_udf_config(name: &str) -> Result<&'static UdfConfig> {
        Ok(&UDF_CONFIGS.get().unwrap()[name])
    }

    pub fn build(&self, udf_name: &str) -> Result<ScalarUDF> {
        let udf_config: &'static UdfConfig = Self::get_udf_config(udf_name)?;

        let pow = make_scalar_function(self.build_function(udf_config));

        let input_types: Vec<DataType> = udf_config
            .input_types
            .clone()
            .into_iter()
            .map(|x| -> Result<DataType> { x.try_into() })
            .collect::<Result<Vec<DataType>>>()?;
        let return_type: DataType = udf_config.return_type.try_into()?;
        Ok(create_udf(
            udf_config.name.as_str(),
            input_types,
            Arc::new(return_type),
            udf_config.volatility.try_into()?,
            pow,
        ))
    }

    fn build_function(
        &self,
        mlconfig: &'static UdfConfig,
    ) -> impl Fn(&[ArrayRef]) -> std::result::Result<ArrayRef, datafusion::error::DataFusionError>
    {
        |args: &[ArrayRef]| {
            let array: Float64Array = Float64Array::from(vec![1.0]);
            let host = mlconfig.host.to_string();

            for arg in args {
                //TODO: map into EntityValue and call api
                let ev = map_array(arg).unwrap();
            }

            Ok(Arc::new(array) as ArrayRef)
        }
    }
}
