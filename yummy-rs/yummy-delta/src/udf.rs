use crate::models::UdfSpec;
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

static UDF_SPECS: OnceCell<HashMap<String, UdfSpec>> = OnceCell::new();

#[derive(Debug)]
pub struct UdfBuilder {}

impl UdfBuilder {
    pub fn init(udf_specs: HashMap<String, UdfSpec>) {
        UDF_SPECS.set(udf_specs).unwrap();
    }

    pub fn get_udf_spec(name: &str) -> Result<&'static UdfSpec> {
        Ok(&UDF_SPECS.get().unwrap()[name])
    }

    pub fn build(&self, udf_name: &str) -> Result<ScalarUDF> {
        let udf_spec: &'static UdfSpec = Self::get_udf_spec(udf_name)?;

        let pow = make_scalar_function(self.build_function(udf_spec));

        let input_types: Vec<DataType> = udf_spec
            .input_types
            .clone()
            .into_iter()
            .map(|x| -> Result<DataType> { x.try_into() })
            .collect::<Result<Vec<DataType>>>()?;
        let return_type: DataType = udf_spec.return_type.try_into()?;
        Ok(create_udf(
            udf_spec.name.as_str(),
            input_types,
            Arc::new(return_type),
            udf_spec.volatility.try_into()?,
            pow,
        ))
    }

    fn build_function(
        &self,
        _mlconfig: &'static UdfSpec,
    ) -> impl Fn(&[ArrayRef]) -> std::result::Result<ArrayRef, datafusion::error::DataFusionError>
    {
        |args: &[ArrayRef]| {
            let arr: Vec<f64> = args[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|x| x.unwrap() + 10.0)
                .collect();

            let array: Float64Array = Float64Array::from(arr);
            //let host = mlconfig.host.to_string();

            /*
            for arg in args {
                //TODO: map into EntityValue and call api
                let ev = map_array(arg).unwrap();
            }
            */

            Ok(Arc::new(array) as ArrayRef)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::UdfConfig;

    #[tokio::test]
    async fn test_init_and_get() -> Result<()> {
        let udf_config = UdfSpec {
            config: UdfConfig::Dummy,
            input_types: vec![crate::models::SchemaPrimitiveType::Float],
            name: "test".to_string(),
            return_type: crate::models::SchemaPrimitiveType::Float,
            volatility: crate::models::Volatility::Immutable,
        };

        UdfBuilder::init(std::collections::HashMap::from([(
            "test".to_string(),
            udf_config,
        )]));
        let udf = UdfBuilder::get_udf_spec("test")?;

        let builder = UdfBuilder {};

        let _scalar_udf = builder.build("test")?;

        assert_eq!(udf.name, "test".to_string());
        Ok(())
    }
}
