use arrow::array::{Array, RecordBatch};
use arrow::compute;
use arrow::datatypes::{DataType, Schema};
use arrow_ord::cmp;
use arrow_select::filter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;
use tracing::{debug, info};

/// Represents a filter condition for predicate pushdown
#[derive(Debug, Clone)]
pub enum FilterCondition {
    /// Column equals value (e.g., "status = 'active'")
    Equals { column: String, value: FilterValue },
    /// Column greater than value (e.g., "age > 18")
    GreaterThan { column: String, value: FilterValue },
    /// Column less than value (e.g., "price < 100.0")
    LessThan { column: String, value: FilterValue },
    /// Column greater than or equal to value
    GreaterThanOrEqual { column: String, value: FilterValue },
    /// Column less than or equal to value
    LessThanOrEqual { column: String, value: FilterValue },
    /// Column not equals value
    NotEquals { column: String, value: FilterValue },
}

/// Supported filter values
#[derive(Debug, Clone)]
pub enum FilterValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
}

/// Configuration for Parquet file reading
#[derive(Debug, Clone)]
pub struct ParquetReaderConfig {
    /// Columns to select (None means select all)
    pub columns: Option<Vec<String>>,
    /// Filter conditions for predicate pushdown
    pub filters: Vec<FilterCondition>,
    /// Maximum number of rows to read (None means read all)
    pub limit: Option<usize>,
}

impl Default for ParquetReaderConfig {
    fn default() -> Self {
        Self {
            columns: None,
            filters: Vec::new(),
            limit: None,
        }
    }
}

/// Parquet file reader with filtering and schema validation
pub struct ParquetReader {
    config: ParquetReaderConfig,
}

impl ParquetReader {
    /// Create a new ParquetReader with the given configuration
    pub fn new(config: ParquetReaderConfig) -> Self {
        Self { config }
    }

    /// Read a Parquet file from the given path
    pub fn read_file(&self, file_path: &str) -> Result<Vec<RecordBatch>, ParquetReaderError> {
        info!("Reading Parquet file: {}", file_path);

        // Open the file
        let file = File::open(file_path)
            .map_err(|e| ParquetReaderError::IoError(format!("Failed to open file {}: {}", file_path, e)))?;

        // Create Parquet reader builder
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ParquetReaderError::ParquetError(format!("Failed to create Parquet reader: {}", e)))?;

        // Get Arrow schema
        let arrow_schema = builder.schema();
        debug!("Arrow schema: {:?}", arrow_schema);

        // Validate requested columns exist in schema
        if let Some(ref columns) = self.config.columns {
            self.validate_columns(&arrow_schema, columns)?;
        }

        // Validate filter columns exist in schema
        self.validate_filter_columns(&arrow_schema)?;

        // Build the reader with batch size
        let reader = builder
            .with_batch_size(1024)
            .build()
            .map_err(|e| ParquetReaderError::ParquetError(format!("Failed to build reader: {}", e)))?;

        let mut batches = Vec::new();
        let mut total_rows = 0;

        // Read all batches
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| ParquetReaderError::ParquetError(format!("Failed to read batch: {}", e)))?;

            // Apply column selection
            let selected_batch = if let Some(ref columns) = self.config.columns {
                self.select_columns(&batch, columns)?
            } else {
                batch
            };

            // Apply filters
            let filtered_batch = self.apply_filters(&selected_batch)?;

            if filtered_batch.num_rows() > 0 {
                total_rows += filtered_batch.num_rows();
                batches.push(filtered_batch);

                // Check limit
                if let Some(limit) = self.config.limit {
                    if total_rows >= limit {
                        // Truncate the last batch if needed
                        if total_rows > limit {
                            let excess = total_rows - limit;
                            let keep_rows = batches.last().unwrap().num_rows() - excess;
                            if let Some(last_batch) = batches.last_mut() {
                                *last_batch = last_batch.slice(0, keep_rows);
                            }
                        }
                        break;
                    }
                }
            }
        }

        info!("Read {} batches with {} total rows from {}", batches.len(), total_rows, file_path);
        Ok(batches)
    }

    /// Validate that requested columns exist in the schema
    fn validate_columns(&self, schema: &Schema, columns: &[String]) -> Result<(), ParquetReaderError> {
        for column in columns {
            if schema.column_with_name(column).is_none() {
                return Err(ParquetReaderError::SchemaError(
                    format!("Column '{}' not found in schema", column)
                ));
            }
        }
        Ok(())
    }

    /// Validate that filter columns exist in the schema
    fn validate_filter_columns(&self, schema: &Schema) -> Result<(), ParquetReaderError> {
        for filter in &self.config.filters {
            let column_name = match filter {
                FilterCondition::Equals { column, .. } |
                FilterCondition::GreaterThan { column, .. } |
                FilterCondition::LessThan { column, .. } |
                FilterCondition::GreaterThanOrEqual { column, .. } |
                FilterCondition::LessThanOrEqual { column, .. } |
                FilterCondition::NotEquals { column, .. } => column,
            };

            if schema.column_with_name(column_name).is_none() {
                return Err(ParquetReaderError::SchemaError(
                    format!("Filter column '{}' not found in schema", column_name)
                ));
            }
        }
        Ok(())
    }

    /// Select specific columns from a record batch
    fn select_columns(&self, batch: &RecordBatch, columns: &[String]) -> Result<RecordBatch, ParquetReaderError> {
        let schema = batch.schema();
        let mut selected_fields = Vec::new();
        let mut selected_arrays = Vec::new();

        for column in columns {
            if let Some((index, field)) = schema.column_with_name(column) {
                selected_fields.push(field.clone());
                selected_arrays.push(batch.column(index).clone());
            } else {
                return Err(ParquetReaderError::SchemaError(
                    format!("Column '{}' not found in batch", column)
                ));
            }
        }

        let selected_schema = Arc::new(Schema::new(selected_fields));
        RecordBatch::try_new(selected_schema, selected_arrays)
            .map_err(|e| ParquetReaderError::ArrowError(format!("Failed to create selected batch: {}", e)))
    }

    /// Apply filter conditions to a record batch
    fn apply_filters(&self, batch: &RecordBatch) -> Result<RecordBatch, ParquetReaderError> {
        if self.config.filters.is_empty() {
            return Ok(batch.clone());
        }

        let mut filter_mask = None;

        for filter in &self.config.filters {
            let column_filter = self.apply_single_filter(batch, filter)?;
            
            filter_mask = match filter_mask {
                None => Some(column_filter),
                Some(existing) => {
                    // AND all filters together
                    let combined = compute::and(&existing, &column_filter)
                        .map_err(|e| ParquetReaderError::FilterError(format!("Failed to combine filters: {}", e)))?;
                    Some(combined)
                }
            };
        }

        if let Some(mask) = filter_mask {
            let filtered_batch = filter::filter_record_batch(batch, &mask)
                .map_err(|e| ParquetReaderError::FilterError(format!("Failed to apply filter: {}", e)))?;
            Ok(filtered_batch)
        } else {
            Ok(batch.clone())
        }
    }

    /// Apply a single filter condition
    fn apply_single_filter(&self, batch: &RecordBatch, filter: &FilterCondition) -> Result<arrow::array::BooleanArray, ParquetReaderError> {
        let schema = batch.schema();
        
        let (column_name, comparison_op, filter_value) = match filter {
            FilterCondition::Equals { column, value } => (column, "eq", value),
            FilterCondition::GreaterThan { column, value } => (column, "gt", value),
            FilterCondition::LessThan { column, value } => (column, "lt", value),
            FilterCondition::GreaterThanOrEqual { column, value } => (column, "gte", value),
            FilterCondition::LessThanOrEqual { column, value } => (column, "lte", value),
            FilterCondition::NotEquals { column, value } => (column, "neq", value),
        };

        let (column_index, field) = schema.column_with_name(column_name)
            .ok_or_else(|| ParquetReaderError::FilterError(format!("Column '{}' not found", column_name)))?;

        let column_array = batch.column(column_index);

        // Apply filter based on data type
        match (field.data_type(), filter_value) {
            (DataType::Utf8, FilterValue::String(value)) => {
                let string_array = column_array.as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| ParquetReaderError::FilterError("Failed to downcast to StringArray".to_string()))?;
                
                self.apply_string_filter(string_array, comparison_op, value)
            },
            (DataType::Int64, FilterValue::Int64(value)) => {
                let int_array = column_array.as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| ParquetReaderError::FilterError("Failed to downcast to Int64Array".to_string()))?;
                
                self.apply_int64_filter(int_array, comparison_op, *value)
            },
            (DataType::Float64, FilterValue::Float64(value)) => {
                let float_array = column_array.as_any().downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| ParquetReaderError::FilterError("Failed to downcast to Float64Array".to_string()))?;
                
                self.apply_float64_filter(float_array, comparison_op, *value)
            },
            (DataType::Boolean, FilterValue::Boolean(value)) => {
                let bool_array = column_array.as_any().downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| ParquetReaderError::FilterError("Failed to downcast to BooleanArray".to_string()))?;
                
                self.apply_boolean_filter(bool_array, comparison_op, *value)
            },
            _ => Err(ParquetReaderError::FilterError(
                format!("Unsupported filter combination: {:?} with {:?}", field.data_type(), filter_value)
            )),
        }
    }

    fn apply_string_filter(&self, array: &arrow::array::StringArray, op: &str, value: &str) -> Result<arrow::array::BooleanArray, ParquetReaderError> {
        use arrow::array::Scalar;
        let scalar = Scalar::new(arrow::array::StringArray::from(vec![value]));
        
        match op {
            "eq" => cmp::eq(array, &scalar),
            "neq" => cmp::neq(array, &scalar),
            "gt" => cmp::gt(array, &scalar),
            "lt" => cmp::lt(array, &scalar),
            "gte" => cmp::gt_eq(array, &scalar),
            "lte" => cmp::lt_eq(array, &scalar),
            _ => return Err(ParquetReaderError::FilterError(format!("Unsupported string operation: {}", op))),
        }.map_err(|e| ParquetReaderError::FilterError(format!("String filter failed: {}", e)))
    }

    fn apply_int64_filter(&self, array: &arrow::array::Int64Array, op: &str, value: i64) -> Result<arrow::array::BooleanArray, ParquetReaderError> {
        use arrow::array::Scalar;
        let scalar = Scalar::new(arrow::array::Int64Array::from(vec![value]));
        
        match op {
            "eq" => cmp::eq(array, &scalar),
            "neq" => cmp::neq(array, &scalar),
            "gt" => cmp::gt(array, &scalar),
            "lt" => cmp::lt(array, &scalar),
            "gte" => cmp::gt_eq(array, &scalar),
            "lte" => cmp::lt_eq(array, &scalar),
            _ => return Err(ParquetReaderError::FilterError(format!("Unsupported int64 operation: {}", op))),
        }.map_err(|e| ParquetReaderError::FilterError(format!("Int64 filter failed: {}", e)))
    }

    fn apply_float64_filter(&self, array: &arrow::array::Float64Array, op: &str, value: f64) -> Result<arrow::array::BooleanArray, ParquetReaderError> {
        use arrow::array::Scalar;
        let scalar = Scalar::new(arrow::array::Float64Array::from(vec![value]));
        
        match op {
            "eq" => cmp::eq(array, &scalar),
            "neq" => cmp::neq(array, &scalar),
            "gt" => cmp::gt(array, &scalar),
            "lt" => cmp::lt(array, &scalar),
            "gte" => cmp::gt_eq(array, &scalar),
            "lte" => cmp::lt_eq(array, &scalar),
            _ => return Err(ParquetReaderError::FilterError(format!("Unsupported float64 operation: {}", op))),
        }.map_err(|e| ParquetReaderError::FilterError(format!("Float64 filter failed: {}", e)))
    }

    fn apply_boolean_filter(&self, array: &arrow::array::BooleanArray, op: &str, value: bool) -> Result<arrow::array::BooleanArray, ParquetReaderError> {
        use arrow::array::Scalar;
        let scalar = Scalar::new(arrow::array::BooleanArray::from(vec![value]));
        
        match op {
            "eq" => cmp::eq(array, &scalar),
            "neq" => cmp::neq(array, &scalar),
            _ => return Err(ParquetReaderError::FilterError(format!("Unsupported boolean operation: {}", op))),
        }.map_err(|e| ParquetReaderError::FilterError(format!("Boolean filter failed: {}", e)))
    }
}

/// Errors that can occur during Parquet reading
#[derive(Debug, thiserror::Error)]
pub enum ParquetReaderError {
    #[error("IO error: {0}")]
    IoError(String),
    
    #[error("Parquet error: {0}")]
    ParquetError(String),
    
    #[error("Schema error: {0}")]
    SchemaError(String),
    
    #[error("Arrow error: {0}")]
    ArrowError(String),
    
    #[error("Filter error: {0}")]
    FilterError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn create_test_parquet_file() -> Result<NamedTempFile, Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        
        // Create test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        // Create test data
        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let value_array = arrow::array::Float64Array::from(vec![10.5, 20.0, 15.5, 30.0, 25.5]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )?;

        // Write to Parquet file
        let file = File::create(temp_file.path())?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(temp_file)
    }

    #[test]
    fn test_read_all_data() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = create_test_parquet_file()?;
        let reader = ParquetReader::new(ParquetReaderConfig::default());
        
        let batches = reader.read_file(temp_file.path().to_str().unwrap())?;
        
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].num_columns(), 3);
        
        Ok(())
    }

    #[test]
    fn test_column_selection() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = create_test_parquet_file()?;
        let config = ParquetReaderConfig {
            columns: Some(vec!["id".to_string(), "name".to_string()]),
            ..Default::default()
        };
        let reader = ParquetReader::new(config);
        
        let batches = reader.read_file(temp_file.path().to_str().unwrap())?;
        
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].num_columns(), 2);
        
        Ok(())
    }

    #[test]
    fn test_string_filter() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = create_test_parquet_file()?;
        let config = ParquetReaderConfig {
            filters: vec![FilterCondition::Equals {
                column: "name".to_string(),
                value: FilterValue::String("Alice".to_string()),
            }],
            ..Default::default()
        };
        let reader = ParquetReader::new(config);
        
        let batches = reader.read_file(temp_file.path().to_str().unwrap())?;
        
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        
        Ok(())
    }

    #[test]
    fn test_numeric_filter() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = create_test_parquet_file()?;
        let config = ParquetReaderConfig {
            filters: vec![FilterCondition::GreaterThan {
                column: "id".to_string(),
                value: FilterValue::Int64(3),
            }],
            ..Default::default()
        };
        let reader = ParquetReader::new(config);
        
        let batches = reader.read_file(temp_file.path().to_str().unwrap())?;
        
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2); // id 4 and 5
        
        Ok(())
    }

    #[test]
    fn test_limit() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = create_test_parquet_file()?;
        let config = ParquetReaderConfig {
            limit: Some(3),
            ..Default::default()
        };
        let reader = ParquetReader::new(config);
        
        let batches = reader.read_file(temp_file.path().to_str().unwrap())?;
        
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
        
        Ok(())
    }
}