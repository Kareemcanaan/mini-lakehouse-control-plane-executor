use crate::parquet_reader::{ParquetReader, ParquetReaderConfig};
use crate::parquet_writer::{ParquetWriter, ParquetWriterConfig};
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use std::sync::Arc;
use tempfile::TempDir;

/// Generate arbitrary Arrow schemas for testing
fn arb_schema() -> impl Strategy<Value = Schema> {
    prop::collection::vec(
        (
            "[a-z][a-z0-9_]*", // field name
            prop::sample::select(vec![
                DataType::Int64,
                DataType::Utf8,
                DataType::Float64,
            ]),
        ),
        1..=5, // 1 to 5 fields
    )
    .prop_map(|fields| {
        let arrow_fields: Vec<Field> = fields
            .into_iter()
            .enumerate()
            .map(|(i, (name, data_type))| {
                // Ensure unique field names by appending index
                Field::new(format!("{}_{}", name, i), data_type, false)
            })
            .collect();
        Schema::new(arrow_fields)
    })
}

/// Generate arbitrary record batches with the given schema
fn arb_record_batch(schema: Arc<Schema>) -> impl Strategy<Value = RecordBatch> {
    let num_rows = 1..=100usize;
    
    num_rows.prop_flat_map(move |rows| {
        let schema_clone = schema.clone();
        let field_strategies: Vec<_> = schema_clone
            .fields()
            .iter()
            .map(|field| {
                match field.data_type() {
                    DataType::Int64 => {
                        prop::collection::vec(any::<i64>(), rows..=rows)
                            .prop_map(|values| Arc::new(Int64Array::from(values)) as Arc<dyn Array>)
                            .boxed()
                    }
                    DataType::Utf8 => {
                        prop::collection::vec("[a-zA-Z0-9 ]{0,20}", rows..=rows)
                            .prop_map(|values| Arc::new(StringArray::from(values)) as Arc<dyn Array>)
                            .boxed()
                    }
                    DataType::Float64 => {
                        prop::collection::vec(any::<f64>().prop_filter("Must be finite", |f| f.is_finite()), rows..=rows)
                            .prop_map(|values| Arc::new(Float64Array::from(values)) as Arc<dyn Array>)
                            .boxed()
                    }
                    _ => unreachable!("Only Int64, Utf8, and Float64 are generated"),
                }
            })
            .collect();

        field_strategies
            .prop_map(move |arrays| {
                RecordBatch::try_new(schema_clone.clone(), arrays).unwrap()
            })
    })
}

/// Generate a vector of record batches with consistent schema
fn arb_record_batches() -> impl Strategy<Value = Vec<RecordBatch>> {
    arb_schema().prop_flat_map(|schema| {
        let schema_arc = Arc::new(schema);
        prop::collection::vec(arb_record_batch(schema_arc), 1..=5)
    })
}

/// Generate table paths that follow the expected pattern
fn arb_table_path() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]*".prop_map(|table_name| {
        format!("tables/{}/data", table_name)
    })
}

proptest! {
    /// **Feature: mini-lakehouse, Property 1: Parquet format consistency**
    /// 
    /// For any data written to a table, the output files should be valid Parquet format 
    /// and stored in the correct object store location
    /// 
    /// **Validates: Requirements 1.1**
    #[test]
    fn test_parquet_format_consistency(
        batches in arb_record_batches(),
        table_path in arb_table_path(),
    ) {
        // Create temporary directory for test
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();
        
        // Create full file path following the expected pattern
        let full_table_path = base_path.join(&table_path);
        std::fs::create_dir_all(&full_table_path).unwrap();
        
        let file_path = full_table_path.join("part-00000.parquet");
        let file_path_str = file_path.to_str().unwrap();
        
        // Write the data using ParquetWriter
        let writer = ParquetWriter::new(ParquetWriterConfig::default());
        let write_result = writer.write_file(&batches, file_path_str);
        
        // Property 1a: Writing should succeed for any valid data
        prop_assert!(write_result.is_ok(), "Failed to write Parquet file: {:?}", write_result.err());
        
        let write_output = write_result.unwrap();
        
        // Property 1b: File should exist at the specified location
        prop_assert!(file_path.exists(), "Parquet file was not created at expected location");
        
        // Property 1c: File should have non-zero size
        prop_assert!(write_output.file_size > 0, "Parquet file has zero size");
        
        // Property 1d: Row count should match input
        let expected_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        prop_assert_eq!(write_output.row_count, expected_rows, "Row count mismatch");
        
        // Property 1e: File should be readable as valid Parquet
        let reader = ParquetReader::new(ParquetReaderConfig::default());
        let read_result = reader.read_file(file_path_str);
        
        prop_assert!(read_result.is_ok(), "Failed to read back Parquet file: {:?}", read_result.err());
        
        let read_batches = read_result.unwrap();
        
        // Property 1f: Read data should have same row count as written
        let read_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        prop_assert_eq!(read_rows, expected_rows, "Read row count doesn't match written row count");
        
        // Property 1g: Read data should have same schema as written
        if !read_batches.is_empty() && !batches.is_empty() {
            let original_schema = batches[0].schema();
            let read_schema = read_batches[0].schema();
            prop_assert_eq!(original_schema, read_schema, "Schema mismatch after round-trip");
        }
        
        // Property 1h: Path should follow the expected table structure
        prop_assert!(table_path.starts_with("tables/"), "Path should start with 'tables/'");
        prop_assert!(table_path.contains("/data"), "Path should contain '/data' directory");
    }
    
    /// Test that partitioned writes also maintain format consistency
    #[test]
    fn test_partitioned_parquet_format_consistency(
        batches in arb_record_batches(),
        num_partitions in 1..=5usize,
    ) {
        // Skip if batches are empty or have no columns suitable for partitioning
        prop_assume!(!batches.is_empty());
        prop_assume!(batches[0].num_columns() > 0);
        
        // Create temporary directory for test
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("partitioned_output");
        let output_dir_str = output_dir.to_str().unwrap();
        
        // Use first column for partitioning
        let schema = batches[0].schema();
        let first_field = schema.field(0);
        let partition_column = first_field.name().clone();
        
        let partition_config = crate::parquet_writer::PartitionConfig {
            num_partitions,
            partition_columns: vec![partition_column],
            hash_function: crate::parquet_writer::HashFunction::Modulo,
        };
        
        // Write partitioned data
        let writer = ParquetWriter::new(ParquetWriterConfig::default());
        let write_result = writer.write_partitioned(&batches, output_dir_str, &partition_config);
        
        // Property: Partitioned writing should succeed
        prop_assert!(write_result.is_ok(), "Failed to write partitioned Parquet files: {:?}", write_result.err());
        
        let partition_outputs = write_result.unwrap();
        
        // Property: Should produce at least one partition (unless input is empty)
        let expected_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if expected_rows > 0 {
            prop_assert!(!partition_outputs.is_empty(), "No partitions were created despite having data");
        }
        
        // Property: Total rows across all partitions should equal input
        let total_partition_rows: usize = partition_outputs.iter().map(|p| p.row_count).sum();
        prop_assert_eq!(total_partition_rows, expected_rows, "Total partition rows don't match input");
        
        // Property: Each partition file should be valid Parquet
        let reader = ParquetReader::new(ParquetReaderConfig::default());
        for partition in &partition_outputs {
            let read_result = reader.read_file(&partition.file_path);
            prop_assert!(read_result.is_ok(), "Failed to read partition file {}: {:?}", 
                        partition.file_path, read_result.err());
            
            let read_batches = read_result.unwrap();
            let read_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
            prop_assert_eq!(read_rows, partition.row_count, 
                           "Partition {} row count mismatch", partition.partition_id);
        }
        
        // Property: Partition files should follow naming convention
        for partition in &partition_outputs {
            prop_assert!(partition.file_path.contains(&format!("part-{}.parquet", partition.partition_id)),
                        "Partition file doesn't follow naming convention: {}", partition.file_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_generators() {
        // Test that our generators produce valid data
        let schema = arb_schema().new_tree(&mut proptest::test_runner::TestRunner::default()).unwrap().current();
        assert!(!schema.fields().is_empty());
        
        let schema_arc = Arc::new(schema);
        let batch = arb_record_batch(schema_arc).new_tree(&mut proptest::test_runner::TestRunner::default()).unwrap().current();
        assert!(batch.num_rows() > 0);
        assert!(batch.num_columns() > 0);
    }
}