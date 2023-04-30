use crate::models::MLModelConfig;
use async_trait::async_trait;
use datafusion::physical_plan::udf::ScalarUDF;
use yummy_core::common::Result;

#[async_trait]
pub trait UdfBuilder {
    async fn build(&self) -> Result<ScalarUDF>;
}

/*
#[async_trait]
impl UdfBuilder for MLModelConfig {
    async fn build(&self) -> Result<ScalarUDF> {}
}
*/
