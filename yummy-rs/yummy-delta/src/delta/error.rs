#[derive(thiserror::Error, Debug)]
pub enum DeltaError {
    #[error("Wrong catboost config")]
    WrongConfig,

    #[error("Store not exists")]
    StoreNotExists,

    #[error("Invalid Schema")]
    InvalidSchema,

    #[error("Please provide numeric or categorical features")]
    ValidationNoFeatures,

    #[error("Wrong number of numeric features (required {0})")]
    ValidationWrongNumericFeatures(i32),

    #[error("Can't convert type")]
    TypeConversionError,

    #[error("Unknown type")]
    UnknownTypeError,

    #[error("Write batch data error")]
    WriteBatchDataError,

    #[error("Wrong write bath data, lack of {0} column")]
    WriteBatchNoColumnError(String),

    #[error("Can't convert type, try cast to primitive type eg. CAST(col1 as STRING)")]
    ReadTypeConversionError,
}
