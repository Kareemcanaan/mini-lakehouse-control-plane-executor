use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow_select::take;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use tracing::info;
use uuid::Uuid;

/// Configuration for Parquet file writing
#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    /// Compression algorithm to use
    pub compression: CompressionType,
    /// Row group size (number of rows per row group)
    pub row_group_size: usize,
    /// Page size in bytes
    pub page_size: usize,
    /// Whether to enable dictionary encoding
    pub enable_dictionary: bool,
    /// Whether to enable statistics
    pub enable_statistics: bool,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            compression: CompressionType::Snappy,
            row_group_size: 1024 * 1024, // 1M rows per row group
            page_size: 1024 * 1024,      // 1MB page size
            enable_dictionary: true,
            enable_statistics: true,
        }
    }
}

/// Supported compression types
#[derive(Debug, Clone)]
pub enum CompressionType {
    None,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
}

impl From<CompressionType> for parquet::basic::Compression {
    fn from(compression: CompressionType) -> Self {
        match compression {
            CompressionType::None => parquet::basic::Compression::UNCOMPRESSED,
            CompressionType::Snappy => parquet::basic::Compression::SNAPPY,
            CompressionType::Gzip => parquet::basic::Compression::GZIP(Default::default()),
            CompressionType::Lz4 => parquet::basic::Compression::LZ4,
            CompressionType::Zstd => parquet::basic::Compression::ZSTD(Default::default()),
        }
    }
}

/// Partitioning configuration for shuffle operations
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    /// Number of partitions to create
    pub num_partitions: usize,
    /// Columns to partition by
    pub partition_columns: Vec<String>,
    /// Hash function to use for partitioning
    pub hash_function: HashFunction,
}

/// Supported hash functions for partitioning
#[derive(Debug, Clone)]
pub enum HashFunction {
    /// Simple modulo hash
    Modulo,
    /// Murmur3 hash (better distribution)
    Murmur3,
}

/// Represents a partition output
#[derive(Debug, Clone)]
pub struct PartitionOutput {
    /// Partition number
    pub partition_id: usize,
    /// File path where partition was written
    pub file_path: String,
    /// Number of rows in this partition
    pub row_count: usize,
    /// File size in bytes
    pub file_size: u64,
}

/// Parquet file writer with partitioning support
pub struct ParquetWriter {
    config: ParquetWriterConfig,
}

impl ParquetWriter {
    /// Create a new ParquetWriter with the given configuration
    pub fn new(config: ParquetWriterConfig) -> Self {
        Self { config }
    }

    /// Write a single Parquet file from record batches
    pub fn write_file(
        &self,
        batches: &[RecordBatch],
        output_path: &str,
    ) -> Result<WriterOutput, ParquetWriterError> {
        if batches.is_empty() {
            return Err(ParquetWriterError::InvalidInput("No batches to write".to_string()));
        }

        info!("Writing Parquet file to: {}", output_path);

        // Ensure output directory exists
        if let Some(parent) = Path::new(output_path).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| ParquetWriterError::IoError(format!("Failed to create directory: {}", e)))?;
        }

        // Get schema from first batch
        let schema = batches[0].schema();
        
        // Validate all batches have the same schema
        for (i, batch) in batches.iter().enumerate() {
            if batch.schema() != schema {
                return Err(ParquetWriterError::SchemaError(
                    format!("Batch {} has different schema than batch 0", i)
                ));
            }
        }

        // Create writer properties
        let props = WriterProperties::builder()
            .set_compression(self.config.compression.clone().into())
            .set_max_row_group_size(self.config.row_group_size)
            .set_data_page_size_limit(self.config.page_size)
            .set_dictionary_enabled(self.config.enable_dictionary)
            .set_statistics_enabled(if self.config.enable_statistics {
                parquet::file::properties::EnabledStatistics::Page
            } else {
                parquet::file::properties::EnabledStatistics::None
            })
            .build();

        // Create file and writer
        let file = File::create(output_path)
            .map_err(|e| ParquetWriterError::IoError(format!("Failed to create file {}: {}", output_path, e)))?;

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| ParquetWriterError::ParquetError(format!("Failed to create Arrow writer: {}", e)))?;

        // Write all batches
        let mut total_rows = 0;
        for batch in batches {
            writer.write(batch)
                .map_err(|e| ParquetWriterError::ParquetError(format!("Failed to write batch: {}", e)))?;
            total_rows += batch.num_rows();
        }

        // Close writer and get metadata
        let _metadata = writer.close()
            .map_err(|e| ParquetWriterError::ParquetError(format!("Failed to close writer: {}", e)))?;

        // Get file size
        let file_size = std::fs::metadata(output_path)
            .map_err(|e| ParquetWriterError::IoError(format!("Failed to get file metadata: {}", e)))?
            .len();

        info!("Wrote {} rows to {} ({} bytes)", total_rows, output_path, file_size);

        Ok(WriterOutput {
            file_path: output_path.to_string(),
            row_count: total_rows,
            file_size,
            schema: schema.as_ref().clone(),
        })
    }

    /// Write partitioned Parquet files for shuffle operations
    pub fn write_partitioned(
        &self,
        batches: &[RecordBatch],
        output_dir: &str,
        partition_config: &PartitionConfig,
    ) -> Result<Vec<PartitionOutput>, ParquetWriterError> {
        if batches.is_empty() {
            return Err(ParquetWriterError::InvalidInput("No batches to write".to_string()));
        }

        info!("Writing partitioned Parquet files to: {} with {} partitions", 
              output_dir, partition_config.num_partitions);

        // Ensure output directory exists
        std::fs::create_dir_all(output_dir)
            .map_err(|e| ParquetWriterError::IoError(format!("Failed to create directory: {}", e)))?;

        // Get schema from first batch
        let schema = batches[0].schema();

        // Validate partition columns exist in schema
        for column in &partition_config.partition_columns {
            if schema.column_with_name(column).is_none() {
                return Err(ParquetWriterError::SchemaError(
                    format!("Partition column '{}' not found in schema", column)
                ));
            }
        }

        // Partition the data
        let partitioned_batches = self.partition_batches(batches, partition_config)?;

        // Write each partition
        let mut partition_outputs = Vec::new();
        for (partition_id, partition_batches) in partitioned_batches.into_iter().enumerate() {
            if partition_batches.is_empty() {
                continue; // Skip empty partitions
            }

            let partition_file = format!("{}/part-{}.parquet", output_dir, partition_id);
            let output = self.write_file(&partition_batches, &partition_file)?;

            partition_outputs.push(PartitionOutput {
                partition_id,
                file_path: partition_file,
                row_count: output.row_count,
                file_size: output.file_size,
            });
        }

        info!("Wrote {} partitions to {}", partition_outputs.len(), output_dir);
        Ok(partition_outputs)
    }

    /// Partition record batches based on partition configuration
    fn partition_batches(
        &self,
        batches: &[RecordBatch],
        partition_config: &PartitionConfig,
    ) -> Result<Vec<Vec<RecordBatch>>, ParquetWriterError> {
        let num_partitions = partition_config.num_partitions;
        let mut partitioned_batches: Vec<Vec<RecordBatch>> = vec![Vec::new(); num_partitions];

        for batch in batches {
            // Calculate partition for each row
            let partition_assignments = self.calculate_partitions(batch, partition_config)?;

            // Group rows by partition
            let mut partition_indices: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];
            for (row_idx, partition_id) in partition_assignments.iter().enumerate() {
                partition_indices[*partition_id].push(row_idx);
            }

            // Create batches for each partition
            for (partition_id, indices) in partition_indices.iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }

                // Create a batch with only the rows for this partition
                let partition_batch = self.select_rows(batch, indices)?;
                partitioned_batches[partition_id].push(partition_batch);
            }
        }

        Ok(partitioned_batches)
    }

    /// Calculate which partition each row belongs to
    fn calculate_partitions(
        &self,
        batch: &RecordBatch,
        partition_config: &PartitionConfig,
    ) -> Result<Vec<usize>, ParquetWriterError> {
        let num_rows = batch.num_rows();
        let mut partitions = Vec::with_capacity(num_rows);

        // For simplicity, we'll use the first partition column for partitioning
        // In a real implementation, you might want to combine multiple columns
        if partition_config.partition_columns.is_empty() {
            // If no partition columns specified, use row index modulo
            for i in 0..num_rows {
                partitions.push(i % partition_config.num_partitions);
            }
        } else {
            let column_name = &partition_config.partition_columns[0];
            let schema = batch.schema();
            let (column_index, _) = schema.column_with_name(column_name)
                .ok_or_else(|| ParquetWriterError::SchemaError(
                    format!("Partition column '{}' not found", column_name)
                ))?;

            let column_array = batch.column(column_index);

            // Calculate hash for each value
            for i in 0..num_rows {
                let hash = self.calculate_hash(column_array, i, &partition_config.hash_function)?;
                partitions.push(hash % partition_config.num_partitions);
            }
        }

        Ok(partitions)
    }

    /// Calculate hash for a specific row in an array
    fn calculate_hash(
        &self,
        array: &dyn arrow::array::Array,
        row_index: usize,
        _hash_function: &HashFunction,
    ) -> Result<usize, ParquetWriterError> {
        use arrow::datatypes::DataType;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        match array.data_type() {
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| ParquetWriterError::PartitionError("Failed to downcast to Int64Array".to_string()))?;
                if let Some(value) = int_array.value(row_index).into() {
                    value.hash(&mut hasher);
                }
            },
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| ParquetWriterError::PartitionError("Failed to downcast to StringArray".to_string()))?;
                if let Some(value) = string_array.value(row_index).into() {
                    value.hash(&mut hasher);
                }
            },
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| ParquetWriterError::PartitionError("Failed to downcast to Float64Array".to_string()))?;
                if let Some(value) = float_array.value(row_index).into() {
                    // Convert float to bits for consistent hashing
                    value.to_bits().hash(&mut hasher);
                }
            },
            _ => {
                return Err(ParquetWriterError::PartitionError(
                    format!("Unsupported data type for partitioning: {:?}", array.data_type())
                ));
            }
        }

        Ok(hasher.finish() as usize)
    }

    /// Select specific rows from a record batch
    fn select_rows(&self, batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch, ParquetWriterError> {
        let indices_array = arrow::array::UInt32Array::from(
            indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
        );

        // Take each column individually
        let schema = batch.schema();
        let mut new_columns = Vec::new();

        for column in batch.columns() {
            let taken_column = take::take(column.as_ref(), &indices_array, None)
                .map_err(|e| ParquetWriterError::PartitionError(format!("Failed to take column: {}", e)))?;
            new_columns.push(taken_column);
        }

        RecordBatch::try_new(schema, new_columns)
            .map_err(|e| ParquetWriterError::PartitionError(format!("Failed to create record batch: {}", e)))
    }

    /// Generate a unique filename with UUID
    pub fn generate_unique_filename(prefix: &str, extension: &str) -> String {
        let uuid = Uuid::new_v4();
        format!("{}-{}.{}", prefix, uuid, extension)
    }
}

/// Output information from writing a Parquet file
#[derive(Debug, Clone)]
pub struct WriterOutput {
    /// Path where the file was written
    pub file_path: String,
    /// Number of rows written
    pub row_count: usize,
    /// File size in bytes
    pub file_size: u64,
    /// Schema of the written data
    pub schema: Schema,
}

/// Errors that can occur during Parquet writing
#[derive(Debug, thiserror::Error)]
pub enum ParquetWriterError {
    #[error("IO error: {0}")]
    IoError(String),
    
    #[error("Parquet error: {0}")]
    ParquetError(String),
    
    #[error("Schema error: {0}")]
    SchemaError(String),
    
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    
    #[error("Partition error: {0}")]
    PartitionError(String),
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
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let value_array = arrow::array::Float64Array::from(vec![10.5, 20.0, 15.5, 30.0, 25.5]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        ).unwrap()
    }

    #[test]
    fn test_write_single_file() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let output_path = temp_dir.path().join("test.parquet");
        
        let writer = ParquetWriter::new(ParquetWriterConfig::default());
        let batch = create_test_batch();
        
        let output = writer.write_file(&[batch], output_path.to_str().unwrap())?;
        
        assert_eq!(output.row_count, 5);
        assert!(output.file_size > 0);
        assert!(output_path.exists());
        
        Ok(())
    }

    #[test]
    fn test_write_partitioned() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let output_dir = temp_dir.path().join("partitioned");
        
        let writer = ParquetWriter::new(ParquetWriterConfig::default());
        let batch = create_test_batch();
        
        let partition_config = PartitionConfig {
            num_partitions: 3,
            partition_columns: vec!["id".to_string()],
            hash_function: HashFunction::Modulo,
        };
        
        let outputs = writer.write_partitioned(
            &[batch], 
            output_dir.to_str().unwrap(), 
            &partition_config
        )?;
        
        assert!(!outputs.is_empty());
        assert!(outputs.len() <= 3); // May be fewer if some partitions are empty
        
        // Verify total row count
        let total_rows: usize = outputs.iter().map(|o| o.row_count).sum();
        assert_eq!(total_rows, 5);
        
        Ok(())
    }

    #[test]
    fn test_generate_unique_filename() {
        let filename1 = ParquetWriter::generate_unique_filename("part", "parquet");
        let filename2 = ParquetWriter::generate_unique_filename("part", "parquet");
        
        assert_ne!(filename1, filename2);
        assert!(filename1.starts_with("part-"));
        assert!(filename1.ends_with(".parquet"));
    }

    #[test]
    fn test_empty_batches_error() {
        let writer = ParquetWriter::new(ParquetWriterConfig::default());
        let result = writer.write_file(&[], "test.parquet");
        
        assert!(result.is_err());
        match result.unwrap_err() {
            ParquetWriterError::InvalidInput(_) => {},
            _ => panic!("Expected InvalidInput error"),
        }
    }
}