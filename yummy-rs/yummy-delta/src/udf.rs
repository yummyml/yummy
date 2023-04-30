use crate::models::MLModelConfig;
use async_trait::async_trait;
use datafusion::arrow::{
    array::{ArrayRef, Float64Array},
    datatypes::DataType,
};
use datafusion::physical_plan::{functions::make_scalar_function, udf::ScalarUDF};
use datafusion_expr::create_udf;
use std::sync::Arc;
use yummy_core::common::Result;

#[async_trait]
pub trait UdfBuilder {
    fn build(&self) -> Result<ScalarUDF>;
}

#[async_trait]
impl UdfBuilder for MLModelConfig {
    fn build(&self) -> Result<ScalarUDF> {
        let pow = |_args: &[ArrayRef]| {
            let array: Float64Array = Float64Array::from(vec![1.0]);
            Ok(Arc::new(array) as ArrayRef)
        };

        let pow = make_scalar_function(pow);

        let input_types: Vec<DataType> = self
            .input_types
            .clone()
            .into_iter()
            .map(|x| -> Result<DataType> { x.try_into() })
            .collect::<Result<Vec<DataType>>>()?;
        let return_type: DataType = self.return_type.try_into()?;
        Ok(create_udf(
            self.name.as_str(),
            input_types,
            Arc::new(return_type),
            self.volatility.try_into()?,
            pow,
        ))
    }
}

