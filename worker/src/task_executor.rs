use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::parquet_reader::{FilterCondition, FilterValue, ParquetReader, ParquetReaderConfig};
use crate::parquet_writer::{ParquetWriter, ParquetWriterConfig};

/// Task specification for execution
#[derive(Debug, Clone)]
pub struct TaskSpec {
    pub operation: TaskOperation,
    pub input_files: Vec<String>,
    pub filter: Option<String>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Aggregate>,
    pub num_partitions: Option<usize>,
    pub output_path: String,
    pub projection: Vec<String>,
}

/// Types of task operations supported
#[derive(Debug, Clone, PartialEq)]
pub enum TaskOperation {
    Scan,
    MapFilter,
    MapProject,
    MapAgg,
    ReduceAgg,
    Shuffle,
}

/// Aggregate function specification
#[derive(Debug, Clone)]
pub struct Aggregate {
    pub function: AggregateFunction,
    pub column: String,
    pub alias: Option<String>,
}

/// Supported aggregate functions
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl std::str::FromStr for AggregateFunction {
    type Err = TaskExecutorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "count" => Ok(AggregateFunction::Count),
            "sum" => Ok(AggregateFunction::Sum),
            "avg" => Ok(AggregateFunction::Avg),
            "min" => Ok(AggregateFunction::Min),
            "max" => Ok(AggregateFunction::Max),
            _ => Err(TaskExecutorError::UnsupportedOperation(
                format!("Unsupported aggregate function: {}", s)
            )),
        }
    }
}

/// Task execution result
#[derive(Debug, Clone)]
pub struct TaskResult {
    pub success: bool,
    pub error: Option<String>,
    pub outputs: Vec<TaskOutput>,
    pub metrics: TaskMetrics,
}

/// Task output information
#[derive(Debug, Clone)]
pub struct TaskOutput {
    pub partition: usize,
    pub path: String,
    pub rows: u64,
    pub size: u64,
}

/// Task execution metrics
#[derive(Debug, Clone)]
pub struct TaskMetrics {
    pub duration_ms: u64,
    pub rows_processed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Task executor for processing data operations
pub struct TaskExecutor {
    parquet_reader: ParquetReader,
    parquet_writer: ParquetWriter,
}

impl TaskExecutor {
    /// Create a new task executor
    pub fn new() -> Self {
        Self {
            parquet_reader: ParquetReader::new(ParquetReaderConfig::default()),
            parquet_writer: ParquetWriter::new(ParquetWriterConfig::default()),
        }
    }

    /// Execute a task based on the specification
    pub async fn execute_task(&self, spec: &TaskSpec) -> Result<TaskResult, TaskExecutorError> {
        let start_time = std::time::Instant::now();
        info!("Executing task: {:?}", spec.operation);

        let result = match spec.operation {
            TaskOperation::Scan => self.execute_scan(spec).await,
            TaskOperation::MapFilter => self.execute_map_filter(spec).await,
            TaskOperation::MapProject => self.execute_map_project(spec).await,
            TaskOperation::MapAgg => self.execute_map_agg(spec).await,
            TaskOperation::ReduceAgg => self.execute_reduce_agg(spec).await,
            TaskOperation::Shuffle => self.execute_shuffle(spec).await,
        };

        let duration = start_time.elapsed();
        
        match result {
            Ok(mut task_result) => {
                task_result.metrics.duration_ms = duration.as_millis() as u64;
                info!("Task completed successfully in {}ms", task_result.metrics.duration_ms);
                Ok(task_result)
            }
            Err(e) => {
                warn!("Task failed after {}ms: {}", duration.as_millis(), e);
                Ok(TaskResult {
                    success: false,
                    error: Some(e.to_string()),
                    outputs: vec![],
                    metrics: TaskMetrics {
                        duration_ms: duration.as_millis() as u64,
                        rows_processed: 0,
                        bytes_read: 0,
                        bytes_written: 0,
                    },
                })
            }
        }
    }

    /// Execute a scan operation
    async fn execute_scan(&self, spec: &TaskSpec) -> Result<TaskResult, TaskExecutorError> {
        debug!("Executing scan operation on {} files", spec.input_files.len());

        let mut all_batches = Vec::new();
        let mut total_bytes_read = 0u64;

        // Read all input files
        for input_file in &spec.input_files {
            let batches = self.parquet_reader.read_file(input_file)
                .map_err(|e| TaskExecutorError::IoError(format!("Failed to read {}: {}", input_file, e)))?;
            
            // Estimate bytes read (rough approximation)
            for batch in &batches {
                total_bytes_read += self.estimate_batch_size(batch);
            }
            
            all_batches.extend(batches);
        }

        let total_rows = all_batches.iter().map(|b| b.num_rows() as u64).sum();

        // Write output
        let output = self.parquet_writer.write_file(&all_batches, &spec.output_path)
            .map_err(|e| TaskExecutorError::IoError(format!("Failed to write output: {}", e)))?;

        Ok(TaskResult {
            success: true,
            error: None,
            outputs: vec![TaskOutput {
                partition: 0,
                path: spec.output_path.clone(),
                rows: output.row_count as u64,
                size: output.file_size,
            }],
            metrics: TaskMetrics {
                duration_ms: 0, // Will be set by caller
                rows_processed: total_rows,
                bytes_read: total_bytes_read,
                bytes_written: output.file_size,
            },
        })
    }

    /// Execute a map filter operation
    async fn execute_map_filter(&self, spec: &TaskSpec) -> Result<TaskResult, TaskExecutorError> {
        debug!("Executing map filter operation");

        if spec.filter.is_none() {
            return Err(TaskExecutorError::InvalidSpec("Filter operation requires filter condition".to_string()));
        }

        // Parse filter condition
        let filter_condition = self.parse_filter_condition(spec.filter.as_ref().unwrap())?;
        
        // Create reader config with filter
        let reader_config = ParquetReaderConfig {
            columns: if spec.projection.is_empty() { None } else { Some(spec.projection.clone()) },
            filters: vec![filter_condition],
            limit: None,
        };
        
        let reader = ParquetReader::new(reader_config);
        let mut all_batches = Vec::new();
        let mut total_bytes_read = 0u64;

        // Read and filter all input files
        for input_file in &spec.input_files {
            let batches = reader.read_file(input_file)
                .map_err(|e| TaskExecutorError::IoError(format!("Failed to read {}: {}", input_file, e)))?;
            
            for batch in &batches {
                total_bytes_read += self.estimate_batch_size(batch);
            }
            
            all_batches.extend(batches);
        }

        let total_rows = all_batches.iter().map(|b| b.num_rows() as u64).sum();

        // Write output
        let output = self.parquet_writer.write_file(&all_batches, &spec.output_path)
            .map_err(|e| TaskExecutorError::IoError(format!("Failed to write output: {}", e)))?;

        Ok(TaskResult {
            success: true,
            error: None,
            outputs: vec![TaskOutput {
                partition: 0,
                path: spec.output_path.clone(),
                rows: output.row_count as u64,
                size: output.file_size,
            }],
            metrics: TaskMetrics {
                duration_ms: 0,
                rows_processed: total_rows,
                bytes_read: total_bytes_read,
                bytes_written: output.file_size,
            },
        })
    }

    /// Execute a map project operation
    async fn execute_map_project(&self, spec: &TaskSpec) -> Result<TaskResult, TaskExecutorError> {
        debug!("Executing map project operation");

        if spec.projection.is_empty() {
            return Err(TaskExecutorError::InvalidSpec("Project operation requires projection columns".to_string()));
        }

        // Create reader config with projection
        let reader_config = ParquetReaderConfig {
            columns: Some(spec.projection.clone()),
            filters: vec![],
            limit: None,
        };
        
        let reader = ParquetReader::new(reader_config);
        let mut all_batches = Vec::new();
        let mut total_bytes_read = 0u64;

        // Read and project all input files
        for input_file in &spec.input_files {
            let batches = reader.read_file(input_file)
                .map_err(|e| TaskExecutorError::IoError(format!("Failed to read {}: {}", input_file, e)))?;
            
            for batch in &batches {
                total_bytes_read += self.estimate_batch_size(batch);
            }
            
            all_batches.extend(batches);
        }

        let total_rows = all_batches.iter().map(|b| b.num_rows() as u64).sum();

        // Write output
        let output = self.parquet_writer.write_file(&all_batches, &spec.output_path)
            .map_err(|e| TaskExecutorError::IoError(format!("Failed to write output: {}", e)))?;

        Ok(TaskResult {
            success: true,
            error: None,
            outputs: vec![TaskOutput {
                partition: 0,
                path: spec.output_path.clone(),
                rows: output.row_count as u64,
                size: output.file_size,
            }],
            metrics: TaskMetrics {
                duration_ms: 0,
                rows_processed: total_rows,
                bytes_read: total_bytes_read,
                bytes_written: output.file_size,
            },
        })
    }

    /// Execute a map aggregation operation
    async fn execute_map_agg(&self, spec: &TaskSpec) -> Result<TaskResult, TaskExecutorError> {
        debug!("Executing map aggregation operation");

        if spec.group_by.is_empty() || spec.aggregates.is_empty() {
            return Err(TaskExecutorError::InvalidSpec("Aggregation requires group_by columns and aggregates".to_string()));
        }

        let mut all_batches = Vec::new();
        let mut total_bytes_read = 0u64;

        // Read all input files
        for input_file in &spec.input_files {
            let batches = self.parquet_reader.read_file(input_file)
                .map_err(|e| TaskExecutorError::IoError(format!("Failed to read {}: {}", input_file, e)))?;
            
            for batch in &batches {
                total_bytes_read += self.estimate_batch_size(batch);
            }
            
            all_batches.extend(batches);
        }

        // Perform aggregation
        let aggregated_batch = self.perform_group_by_aggregation(&all_batches, &spec.group_by, &spec.aggregates)?;
        let total_rows = aggregated_batch.num_rows() as u64;

        // Write output
        let output = self.parquet_writer.write_file(&[aggregated_batch], &spec.output_path)
            .map_err(|e| TaskExecutorError::IoError(format!("Failed to write output: {}", e)))?;

        Ok(TaskResult {
            success: true,
            error: None,
            outputs: vec![TaskOutput {
                partition: 0,
                path: spec.output_path.clone(),
                rows: output.row_count as u64,
                size: output.file_size,
            }],
            metrics: TaskMetrics {
                duration_ms: 0,
                rows_processed: total_rows,
                bytes_read: total_bytes_read,
                bytes_written: output.file_size,
            },
        })
    }

    /// Execute a reduce aggregation operation
    async fn execute_reduce_agg(&self, spec: &TaskSpec) -> Result<TaskResult, TaskExecutorError> {
        debug!("Executing reduce aggregation operation");

        // For reduce aggregation, we combine partial aggregates from map stage
        let mut all_batches = Vec::new();
        let mut total_bytes_read = 0u64;

        // Read all input files (partial aggregates)
        for input_file in &spec.input_files {
            let batches = self.parquet_reader.read_file(input_file)
                .map_err(|e| TaskExecutorError::IoError(format!("Failed to read {}: {}", input_file, e)))?;
            
            for batch in &batches {
                total_bytes_read += self.estimate_batch_size(batch);
            }
            
            all_batches.extend(batches);
        }

        // Combine partial aggregates
        let final_batch = self.combine_partial_aggregates(&all_batches, &spec.group_by, &spec.aggregates)?;
        let total_rows = final_batch.num_rows() as u64;

        // Write output
        let output = self.parquet_writer.write_file(&[final_batch], &spec.output_path)
            .map_err(|e| TaskExecutorError::IoError(format!("Failed to write output: {}", e)))?;

        Ok(TaskResult {
            success: true,
            error: None,
            outputs: vec![TaskOutput {
                partition: 0,
                path: spec.output_path.clone(),
                rows: output.row_count as u64,
                size: output.file_size,
            }],
            metrics: TaskMetrics {
                duration_ms: 0,
                rows_processed: total_rows,
                bytes_read: total_bytes_read,
                bytes_written: output.file_size,
            },
        })
    }

    /// Execute a shuffle operation
    async fn execute_shuffle(&self, spec: &TaskSpec) -> Result<TaskResult, TaskExecutorError> {
        debug!("Executing shuffle operation");

        if spec.num_partitions.is_none() {
            return Err(TaskExecutorError::InvalidSpec("Shuffle operation requires num_partitions".to_string()));
        }

        let num_partitions = spec.num_partitions.unwrap();
        let mut all_batches = Vec::new();
        let mut total_bytes_read = 0u64;

        // Read all input files
        for input_file in &spec.input_files {
            let batches = self.parquet_reader.read_file(input_file)
                .map_err(|e| TaskExecutorError::IoError(format!("Failed to read {}: {}", input_file, e)))?;
            
            for batch in &batches {
                total_bytes_read += self.estimate_batch_size(batch);
            }
            
            all_batches.extend(batches);
        }

        // Create partition configuration
        let partition_config = crate::parquet_writer::PartitionConfig {
            num_partitions,
            partition_columns: if spec.group_by.is_empty() {
                // If no group by columns, use first column for partitioning
                if !all_batches.is_empty() && all_batches[0].num_columns() > 0 {
                    vec![all_batches[0].schema().field(0).name().clone()]
                } else {
                    vec![]
                }
            } else {
                spec.group_by.clone()
            },
            hash_function: crate::parquet_writer::HashFunction::Modulo,
        };

        // Write partitioned output with attempt isolation
        let partition_outputs = self.parquet_writer.write_partitioned(
            &all_batches,
            &spec.output_path,
            &partition_config,
        ).map_err(|e| TaskExecutorError::IoError(format!("Failed to write partitioned output: {}", e)))?;

        // Convert partition outputs to task outputs
        let task_outputs: Vec<TaskOutput> = partition_outputs.into_iter().map(|p| TaskOutput {
            partition: p.partition_id,
            path: p.file_path,
            rows: p.row_count as u64,
            size: p.file_size,
        }).collect();

        let total_rows_processed = all_batches.iter().map(|b| b.num_rows() as u64).sum();
        let total_bytes_written = task_outputs.iter().map(|o| o.size).sum();

        Ok(TaskResult {
            success: true,
            error: None,
            outputs: task_outputs,
            metrics: TaskMetrics {
                duration_ms: 0, // Will be set by caller
                rows_processed: total_rows_processed,
                bytes_read: total_bytes_read,
                bytes_written: total_bytes_written,
            },
        })
    }

    /// Parse a simple filter condition from string
    fn parse_filter_condition(&self, filter_str: &str) -> Result<FilterCondition, TaskExecutorError> {
        // Simple parser for basic conditions like "column > value" or "column = 'value'"
        let parts: Vec<&str> = filter_str.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(TaskExecutorError::InvalidSpec(
                format!("Invalid filter format: {}", filter_str)
            ));
        }

        let column = parts[0].to_string();
        let operator = parts[1];
        let value_str = parts[2];

        // Parse value (simple heuristic)
        let value = if value_str.starts_with('\'') && value_str.ends_with('\'') {
            // String value
            FilterValue::String(value_str[1..value_str.len()-1].to_string())
        } else if let Ok(int_val) = value_str.parse::<i64>() {
            FilterValue::Int64(int_val)
        } else if let Ok(float_val) = value_str.parse::<f64>() {
            FilterValue::Float64(float_val)
        } else if let Ok(bool_val) = value_str.parse::<bool>() {
            FilterValue::Boolean(bool_val)
        } else {
            return Err(TaskExecutorError::InvalidSpec(
                format!("Cannot parse filter value: {}", value_str)
            ));
        };

        let condition = match operator {
            "=" | "==" => FilterCondition::Equals { column, value },
            ">" => FilterCondition::GreaterThan { column, value },
            "<" => FilterCondition::LessThan { column, value },
            ">=" => FilterCondition::GreaterThanOrEqual { column, value },
            "<=" => FilterCondition::LessThanOrEqual { column, value },
            "!=" | "<>" => FilterCondition::NotEquals { column, value },
            _ => return Err(TaskExecutorError::InvalidSpec(
                format!("Unsupported filter operator: {}", operator)
            )),
        };

        Ok(condition)
    }

    /// Estimate the size of a record batch in bytes
    fn estimate_batch_size(&self, batch: &RecordBatch) -> u64 {
        let mut size = 0u64;
        for column in batch.columns() {
            size += column.get_array_memory_size() as u64;
        }
        size
    }

    /// Perform GROUP BY aggregation on record batches
    fn perform_group_by_aggregation(
        &self,
        batches: &[RecordBatch],
        group_by_columns: &[String],
        aggregates: &[Aggregate],
    ) -> Result<RecordBatch, TaskExecutorError> {
        if batches.is_empty() {
            return Err(TaskExecutorError::AggregationError("No batches to aggregate".to_string()));
        }

        let schema = batches[0].schema();
        
        // Validate group by columns exist
        for col in group_by_columns {
            if schema.column_with_name(col).is_none() {
                return Err(TaskExecutorError::SchemaError(
                    format!("Group by column '{}' not found in schema", col)
                ));
            }
        }

        // Validate aggregate columns exist
        for agg in aggregates {
            if agg.column != "*" && schema.column_with_name(&agg.column).is_none() {
                return Err(TaskExecutorError::SchemaError(
                    format!("Aggregate column '{}' not found in schema", agg.column)
                ));
            }
        }

        // For simplicity, we'll implement a basic hash-based aggregation
        // In a production system, you'd want more sophisticated algorithms
        let mut groups: HashMap<Vec<String>, Vec<f64>> = HashMap::new();

        // Process each batch
        for batch in batches {
            let num_rows = batch.num_rows();
            
            // Extract group by column values
            let mut group_columns = Vec::new();
            for col_name in group_by_columns {
                let (col_idx, _) = schema.column_with_name(col_name).unwrap();
                group_columns.push(batch.column(col_idx));
            }

            // Extract aggregate column values
            let mut agg_columns = Vec::new();
            for agg in aggregates {
                if agg.column == "*" {
                    // For COUNT(*), we don't need actual column data
                    agg_columns.push(None);
                } else {
                    let (col_idx, _) = schema.column_with_name(&agg.column).unwrap();
                    agg_columns.push(Some(batch.column(col_idx)));
                }
            }

            // Process each row
            for row_idx in 0..num_rows {
                // Build group key
                let mut group_key = Vec::new();
                for col_array in &group_columns {
                    let key_value = self.extract_string_value(col_array, row_idx)?;
                    group_key.push(key_value);
                }

                // Get or create group entry
                let group_entry = groups.entry(group_key).or_insert_with(|| {
                    vec![0.0; aggregates.len()]
                });

                // Update aggregates for this group
                for (agg_idx, agg) in aggregates.iter().enumerate() {
                    match agg.function {
                        AggregateFunction::Count => {
                            group_entry[agg_idx] += 1.0;
                        }
                        AggregateFunction::Sum => {
                            if let Some(col_array) = &agg_columns[agg_idx] {
                                let value = self.extract_numeric_value(col_array, row_idx)?;
                                group_entry[agg_idx] += value;
                            }
                        }
                        AggregateFunction::Avg => {
                            // For map phase, we store sum and count separately
                            // This is a simplified implementation
                            if let Some(col_array) = &agg_columns[agg_idx] {
                                let value = self.extract_numeric_value(col_array, row_idx)?;
                                group_entry[agg_idx] += value;
                            }
                        }
                        AggregateFunction::Min => {
                            if let Some(col_array) = &agg_columns[agg_idx] {
                                let value = self.extract_numeric_value(col_array, row_idx)?;
                                if group_entry[agg_idx] == 0.0 || value < group_entry[agg_idx] {
                                    group_entry[agg_idx] = value;
                                }
                            }
                        }
                        AggregateFunction::Max => {
                            if let Some(col_array) = &agg_columns[agg_idx] {
                                let value = self.extract_numeric_value(col_array, row_idx)?;
                                if value > group_entry[agg_idx] {
                                    group_entry[agg_idx] = value;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Build result schema
        let mut result_fields = Vec::new();
        
        // Add group by columns
        for col_name in group_by_columns {
            let (_, field) = schema.column_with_name(col_name).unwrap();
            result_fields.push(field.clone());
        }
        
        // Add aggregate columns
        for agg in aggregates {
            let default_alias = format!("{}_{}", agg.function.function_name(), agg.column);
            let alias = agg.alias.as_ref().unwrap_or(&default_alias);
            result_fields.push(Field::new(alias, DataType::Float64, true));
        }

        let result_schema = Arc::new(Schema::new(result_fields));

        // Build result arrays
        let mut result_arrays: Vec<Arc<dyn Array>> = Vec::new();
        
        // Sort groups for deterministic output
        let mut sorted_groups: Vec<_> = groups.into_iter().collect();
        sorted_groups.sort_by(|a, b| a.0.cmp(&b.0));

        // Build group by column arrays
        for (col_idx, col_name) in group_by_columns.iter().enumerate() {
            let (_, field) = schema.column_with_name(col_name).unwrap();
            let values: Vec<String> = sorted_groups.iter().map(|(key, _)| key[col_idx].clone()).collect();
            
            match field.data_type() {
                DataType::Utf8 => {
                    let string_array = arrow::array::StringArray::from(values);
                    result_arrays.push(Arc::new(string_array));
                }
                DataType::Int64 => {
                    let int_values: Result<Vec<i64>, _> = values.iter().map(|s| s.parse()).collect();
                    match int_values {
                        Ok(ints) => {
                            let int_array = arrow::array::Int64Array::from(ints);
                            result_arrays.push(Arc::new(int_array));
                        }
                        Err(_) => {
                            return Err(TaskExecutorError::AggregationError(
                                format!("Failed to parse int64 values for column {}", col_name)
                            ));
                        }
                    }
                }
                _ => {
                    // For other types, convert to string for now
                    let string_array = arrow::array::StringArray::from(values);
                    result_arrays.push(Arc::new(string_array));
                }
            }
        }

        // Build aggregate column arrays
        for agg_idx in 0..aggregates.len() {
            let values: Vec<f64> = sorted_groups.iter().map(|(_, agg_values)| agg_values[agg_idx]).collect();
            let float_array = arrow::array::Float64Array::from(values);
            result_arrays.push(Arc::new(float_array));
        }

        // Create result batch
        RecordBatch::try_new(result_schema, result_arrays)
            .map_err(|e| TaskExecutorError::AggregationError(format!("Failed to create result batch: {}", e)))
    }

    /// Combine partial aggregates from map phase
    fn combine_partial_aggregates(
        &self,
        batches: &[RecordBatch],
        group_by_columns: &[String],
        aggregates: &[Aggregate],
    ) -> Result<RecordBatch, TaskExecutorError> {
        // For reduce phase, we combine the partial results
        // This is similar to the map aggregation but combines existing aggregates
        self.perform_group_by_aggregation(batches, group_by_columns, aggregates)
    }

    /// Extract string value from array at given index
    fn extract_string_value(&self, array: &Arc<dyn Array>, index: usize) -> Result<String, TaskExecutorError> {
        match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| TaskExecutorError::AggregationError("Failed to downcast to StringArray".to_string()))?;
                Ok(string_array.value(index).to_string())
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| TaskExecutorError::AggregationError("Failed to downcast to Int64Array".to_string()))?;
                Ok(int_array.value(index).to_string())
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| TaskExecutorError::AggregationError("Failed to downcast to Float64Array".to_string()))?;
                Ok(float_array.value(index).to_string())
            }
            _ => Err(TaskExecutorError::AggregationError(
                format!("Unsupported data type for grouping: {:?}", array.data_type())
            )),
        }
    }

    /// Extract numeric value from array at given index
    fn extract_numeric_value(&self, array: &Arc<dyn Array>, index: usize) -> Result<f64, TaskExecutorError> {
        match array.data_type() {
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| TaskExecutorError::AggregationError("Failed to downcast to Int64Array".to_string()))?;
                Ok(int_array.value(index) as f64)
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| TaskExecutorError::AggregationError("Failed to downcast to Float64Array".to_string()))?;
                Ok(float_array.value(index))
            }
            DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<arrow::array::Int32Array>()
                    .ok_or_else(|| TaskExecutorError::AggregationError("Failed to downcast to Int32Array".to_string()))?;
                Ok(int_array.value(index) as f64)
            }
            _ => Err(TaskExecutorError::AggregationError(
                format!("Unsupported data type for aggregation: {:?}", array.data_type())
            )),
        }
    }
}

impl AggregateFunction {
    /// Get the function name as a string
    fn function_name(&self) -> &'static str {
        match self {
            AggregateFunction::Count => "count",
            AggregateFunction::Sum => "sum",
            AggregateFunction::Avg => "avg",
            AggregateFunction::Min => "min",
            AggregateFunction::Max => "max",
        }
    }
}

impl Default for TaskExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during task execution
#[derive(Debug, thiserror::Error)]
pub enum TaskExecutorError {
    #[error("IO error: {0}")]
    IoError(String),
    
    #[error("Invalid task specification: {0}")]
    InvalidSpec(String),
    
    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
    
    #[error("Aggregation error: {0}")]
    AggregationError(String),
    
    #[error("Schema error: {0}")]
    SchemaError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("quantity", DataType::Int64, false),
        ]));

        let category_array = StringArray::from(vec!["A", "B", "A", "C", "B", "A"]);
        let amount_array = Int64Array::from(vec![100, 200, 150, 300, 250, 120]);
        let quantity_array = Int64Array::from(vec![1, 2, 1, 3, 2, 1]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(category_array),
                Arc::new(amount_array),
                Arc::new(quantity_array),
            ],
        ).unwrap()
    }

    #[tokio::test]
    async fn test_group_by_aggregation() -> Result<(), Box<dyn std::error::Error>> {
        let executor = TaskExecutor::new();
        let batch = create_test_batch();

        let aggregates = vec![
            Aggregate {
                function: AggregateFunction::Sum,
                column: "amount".to_string(),
                alias: Some("total_amount".to_string()),
            },
            Aggregate {
                function: AggregateFunction::Count,
                column: "*".to_string(),
                alias: Some("count".to_string()),
            },
        ];

        let result = executor.perform_group_by_aggregation(
            &[batch],
            &["category".to_string()],
            &aggregates,
        )?;

        // Should have 3 groups: A, B, C
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 3); // category + 2 aggregates

        // Verify schema
        let schema = result.schema();
        assert_eq!(schema.field(0).name(), "category");
        assert_eq!(schema.field(1).name(), "total_amount");
        assert_eq!(schema.field(2).name(), "count");

        Ok(())
    }

    #[test]
    fn test_parse_filter_condition() {
        let executor = TaskExecutor::new();

        // Test string filter
        let filter = executor.parse_filter_condition("name = 'Alice'").unwrap();
        match filter {
            FilterCondition::Equals { column, value } => {
                assert_eq!(column, "name");
                match value {
                    FilterValue::String(s) => assert_eq!(s, "Alice"),
                    _ => panic!("Expected string value"),
                }
            }
            _ => panic!("Expected equals condition"),
        }

        // Test numeric filter
        let filter = executor.parse_filter_condition("age > 18").unwrap();
        match filter {
            FilterCondition::GreaterThan { column, value } => {
                assert_eq!(column, "age");
                match value {
                    FilterValue::Int64(i) => assert_eq!(i, 18),
                    _ => panic!("Expected int64 value"),
                }
            }
            _ => panic!("Expected greater than condition"),
        }

        // Test invalid filter
        let result = executor.parse_filter_condition("invalid filter");
        assert!(result.is_err());
    }

    #[test]
    fn test_aggregate_function_from_str() {
        assert_eq!("count".parse::<AggregateFunction>().unwrap(), AggregateFunction::Count);
        assert_eq!("sum".parse::<AggregateFunction>().unwrap(), AggregateFunction::Sum);
        assert_eq!("avg".parse::<AggregateFunction>().unwrap(), AggregateFunction::Avg);
        assert_eq!("min".parse::<AggregateFunction>().unwrap(), AggregateFunction::Min);
        assert_eq!("max".parse::<AggregateFunction>().unwrap(), AggregateFunction::Max);

        // Test case insensitive
        assert_eq!("COUNT".parse::<AggregateFunction>().unwrap(), AggregateFunction::Count);
        assert_eq!("Sum".parse::<AggregateFunction>().unwrap(), AggregateFunction::Sum);

        // Test invalid function
        let result = "invalid".parse::<AggregateFunction>();
        assert!(result.is_err());
    }

    #[test]
    fn test_function_name() {
        assert_eq!(AggregateFunction::Count.function_name(), "count");
        assert_eq!(AggregateFunction::Sum.function_name(), "sum");
        assert_eq!(AggregateFunction::Avg.function_name(), "avg");
        assert_eq!(AggregateFunction::Min.function_name(), "min");
        assert_eq!(AggregateFunction::Max.function_name(), "max");
    }

    #[tokio::test]
    async fn test_shuffle_operation() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::TempDir::new()?;
        let input_file = temp_dir.path().join("input.parquet");
        let output_dir = temp_dir.path().join("shuffle_output");

        // Create test data and write to input file
        let executor = TaskExecutor::new();
        let batch = create_test_batch();
        
        // Write input file
        executor.parquet_writer.write_file(&[batch], input_file.to_str().unwrap())?;

        // Create shuffle task spec
        let spec = TaskSpec {
            operation: TaskOperation::Shuffle,
            input_files: vec![input_file.to_str().unwrap().to_string()],
            filter: None,
            group_by: vec!["category".to_string()],
            aggregates: vec![],
            num_partitions: Some(3),
            output_path: output_dir.to_str().unwrap().to_string(),
            projection: vec![],
        };

        // Execute shuffle
        let result = executor.execute_task(&spec).await?;

        // Verify result
        assert!(result.success);
        assert!(result.error.is_none());
        assert!(!result.outputs.is_empty());
        assert!(result.outputs.len() <= 3); // May be fewer if some partitions are empty

        // Verify total row count is preserved
        let total_output_rows: u64 = result.outputs.iter().map(|o| o.rows).sum();
        assert_eq!(total_output_rows, 6); // Original batch had 6 rows

        Ok(())
    }
}