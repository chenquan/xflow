use datafusion::arrow::array::Float64Array;
use datafusion::arrow::error::ArrowError;
use datafusion::common::downcast_value;
use datafusion::logical_expr::{create_udf, ColumnarValue};

fn json(args: &[ColumnarValue]) -> Result<ColumnarValue, ArrowError> {
    let args = ColumnarValue::values_to_arrays(args)?;

    // 1. cast both arguments to f64. These casts MUST be aligned with the signature or this function panics!
    let base = as_float64_array(&args[0]).expect("cast failed");
    let exponent = as_float64_array(&args[1]).expect("cast failed");
    create_udf()
}
fn as_float64_array(columnar: &ColumnarValue) -> Result<&Float64Array, ArrowError> {
    downcast_value!(columnar,  Float64Array)
}