use crate::task_executor::{TaskExecutor, Aggregate, AggregateFunction};
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use std::collections::HashMap;
use std::sync::Arc;

/// Generate arbitrary schemas suitable for GROUP BY operations
fn arb_groupby_schema() -> impl Strategy<Value = Schema> {
    // Create schemas with at least one grouping column and one numeric column
    (
        prop::collection::vec("[a-z][a-z0-9_]*", 1..=3), // group by columns (strings)
        prop::collection::vec("[a-z][a-z0-9_]*", 1..=3), // numeric columns for aggregation
    ).prop_map(|(group_cols, agg_cols)| {
        let mut fields = Vec::new();
        
        // Add group by columns as strings
        for (i, _col_name) in group_cols.into_iter().enumerate() {
            fields.push(Field::new(format!("group_{}", i), DataType::Utf8, false));
        }
        
        // Add numeric columns for aggregation
        for (i, _col_name) in agg_cols.into_iter().enumerate() {
            fields.push(Field::new(format!("value_{}", i), DataType::Int64, false));
        }
        
        Schema::new(fields)
    })
}

/// Generate record batches with controlled group cardinality for predictable aggregation
fn arb_groupby_record_batch(schema: Arc<Schema>) -> impl Strategy<Value = RecordBatch> {
    let num_rows = 10..=50usize;
    
    num_rows.prop_flat_map(move |rows| {
        let schema_clone = schema.clone();
        
        // Generate limited set of group values for predictable results
        let group_values = prop::collection::vec("[ABC]", 1..=5); // Limited alphabet for groups
        
        group_values.prop_flat_map(move |possible_groups| {
            let schema_clone2 = schema_clone.clone();
            
            let field_strategies: Vec<_> = schema_clone2
                .fields()
                .iter()
                .map(|field| {
                    match field.data_type() {
                        DataType::Utf8 => {
                            // Use limited group values for predictable aggregation
                            prop::collection::vec(
                                prop::sample::select(possible_groups.clone()), 
                                rows..=rows
                            )
                            .prop_map(|values| Arc::new(StringArray::from(values)) as Arc<dyn Array>)
                            .boxed()
                        }
                        DataType::Int64 => {
                            // Use small positive integers for easier verification
                            prop::collection::vec(1i64..=100, rows..=rows)
                                .prop_map(|values| Arc::new(Int64Array::from(values)) as Arc<dyn Array>)
                                .boxed()
                        }
                        _ => unreachable!("Only Utf8 and Int64 are generated"),
                    }
                })
                .collect();

            field_strategies
                .prop_map(move |arrays| {
                    RecordBatch::try_new(schema_clone2.clone(), arrays).unwrap()
                })
        })
    })
}

/// Generate a vector of record batches with consistent schema for GROUP BY testing
fn arb_groupby_record_batches() -> impl Strategy<Value = Vec<RecordBatch>> {
    arb_groupby_schema().prop_flat_map(|schema| {
        let schema_arc = Arc::new(schema);
        prop::collection::vec(arb_groupby_record_batch(schema_arc), 1..=3)
    })
}

/// Calculate expected GROUP BY results manually for verification
fn calculate_expected_results(
    batches: &[RecordBatch],
    group_by_columns: &[String],
    aggregates: &[Aggregate],
) -> HashMap<Vec<String>, Vec<f64>> {
    let mut groups: HashMap<Vec<String>, Vec<f64>> = HashMap::new();
    
    for batch in batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        
        // Get group by column indices and arrays
        let mut group_column_arrays = Vec::new();
        for col_name in group_by_columns {
            let (col_idx, _) = schema.column_with_name(col_name).unwrap();
            group_column_arrays.push(batch.column(col_idx));
        }
        
        // Get aggregate column arrays
        let mut agg_column_arrays = Vec::new();
        for agg in aggregates {
            if agg.column == "*" {
                agg_column_arrays.push(None);
            } else {
                let (col_idx, _) = schema.column_with_name(&agg.column).unwrap();
                agg_column_arrays.push(Some(batch.column(col_idx)));
            }
        }
        
        // Process each row
        for row_idx in 0..num_rows {
            // Build group key
            let mut group_key = Vec::new();
            for col_array in &group_column_arrays {
                let string_array = col_array.as_any().downcast_ref::<StringArray>().unwrap();
                group_key.push(string_array.value(row_idx).to_string());
            }
            
            // Get or create group entry
            let group_entry = groups.entry(group_key).or_insert_with(|| {
                vec![0.0; aggregates.len()]
            });
            
            // Update aggregates
            for (agg_idx, agg) in aggregates.iter().enumerate() {
                match agg.function {
                    AggregateFunction::Count => {
                        group_entry[agg_idx] += 1.0;
                    }
                    AggregateFunction::Sum => {
                        if let Some(col_array) = &agg_column_arrays[agg_idx] {
                            let int_array = col_array.as_any().downcast_ref::<Int64Array>().unwrap();
                            group_entry[agg_idx] += int_array.value(row_idx) as f64;
                        }
                    }
                    _ => {
                        // For this test, we focus on COUNT and SUM as per requirements
                        panic!("Only COUNT and SUM are tested in this property");
                    }
                }
            }
        }
    }
    
    groups
}

/// Extract actual results from the aggregated record batch
fn extract_actual_results(
    result_batch: &RecordBatch,
    group_by_columns: &[String],
    aggregates: &[Aggregate],
) -> HashMap<Vec<String>, Vec<f64>> {
    let mut results = HashMap::new();
    let num_rows = result_batch.num_rows();
    let schema = result_batch.schema();
    
    // Get group column arrays
    let mut group_arrays = Vec::new();
    for col_name in group_by_columns {
        let (col_idx, _) = schema.column_with_name(col_name).unwrap();
        group_arrays.push(result_batch.column(col_idx));
    }
    
    // Get aggregate column arrays
    let mut agg_arrays = Vec::new();
    for agg in aggregates {
        let default_alias = format!("{}_{}", agg.function.function_name(), agg.column);
        let alias = agg.alias.as_ref().unwrap_or(&default_alias);
        let (col_idx, _) = schema.column_with_name(alias).unwrap();
        agg_arrays.push(result_batch.column(col_idx));
    }
    
    // Extract results for each row
    for row_idx in 0..num_rows {
        // Build group key
        let mut group_key = Vec::new();
        for col_array in &group_arrays {
            match col_array.data_type() {
                DataType::Utf8 => {
                    let string_array = col_array.as_any().downcast_ref::<StringArray>().unwrap();
                    group_key.push(string_array.value(row_idx).to_string());
                }
                DataType::Int64 => {
                    let int_array = col_array.as_any().downcast_ref::<Int64Array>().unwrap();
                    group_key.push(int_array.value(row_idx).to_string());
                }
                _ => panic!("Unsupported group key type"),
            }
        }
        
        // Extract aggregate values
        let mut agg_values = Vec::new();
        for col_array in &agg_arrays {
            let float_array = col_array.as_any().downcast_ref::<Float64Array>().unwrap();
            agg_values.push(float_array.value(row_idx));
        }
        
        results.insert(group_key, agg_values);
    }
    
    results
}

proptest! {
    /// **Feature: mini-lakehouse, Property 18: GROUP BY correctness**
    /// 
    /// For any GROUP BY query with count and sum aggregates, the results should match 
    /// expected mathematical outcomes
    /// 
    /// **Validates: Requirements 4.5**
    #[test]
    fn test_group_by_correctness(
        batches in arb_groupby_record_batches(),
    ) {
        // Skip empty batches
        prop_assume!(!batches.is_empty());
        prop_assume!(batches[0].num_rows() > 0);
        prop_assume!(batches[0].num_columns() >= 2); // At least one group column and one value column
        
        let schema = batches[0].schema();
        
        // Find group by columns (string columns)
        let group_by_columns: Vec<String> = schema
            .fields()
            .iter()
            .filter(|field| field.data_type() == &DataType::Utf8)
            .map(|field| field.name().clone())
            .take(1) // Use first string column for grouping
            .collect();
        
        // Find numeric columns for aggregation
        let numeric_columns: Vec<String> = schema
            .fields()
            .iter()
            .filter(|field| field.data_type() == &DataType::Int64)
            .map(|field| field.name().clone())
            .take(1) // Use first numeric column for aggregation
            .collect();
        
        prop_assume!(!group_by_columns.is_empty());
        prop_assume!(!numeric_columns.is_empty());
        
        // Create aggregates: COUNT(*) and SUM(numeric_column)
        let aggregates = vec![
            Aggregate {
                function: AggregateFunction::Count,
                column: "*".to_string(),
                alias: Some("count_star".to_string()),
            },
            Aggregate {
                function: AggregateFunction::Sum,
                column: numeric_columns[0].clone(),
                alias: Some(format!("sum_{}", numeric_columns[0])),
            },
        ];
        
        // Calculate expected results manually
        let expected_results = calculate_expected_results(&batches, &group_by_columns, &aggregates);
        
        // Execute GROUP BY using TaskExecutor
        let executor = TaskExecutor::new();
        let actual_batch = executor.perform_group_by_aggregation(&batches, &group_by_columns, &aggregates);
        
        // Property 18a: GROUP BY execution should succeed
        prop_assert!(actual_batch.is_ok(), "GROUP BY aggregation failed: {:?}", actual_batch.err());
        
        let result_batch = actual_batch.unwrap();
        
        // Property 18b: Result should have correct number of columns
        let expected_columns = group_by_columns.len() + aggregates.len();
        prop_assert_eq!(result_batch.num_columns(), expected_columns, 
                       "Result has wrong number of columns");
        
        // Property 18c: Result should have correct number of groups
        prop_assert_eq!(result_batch.num_rows(), expected_results.len(),
                       "Result has wrong number of groups");
        
        // Extract actual results from the result batch
        let actual_results = extract_actual_results(&result_batch, &group_by_columns, &aggregates);
        
        // Property 18d: All expected groups should be present in results
        for (expected_group, expected_values) in &expected_results {
            prop_assert!(actual_results.contains_key(expected_group),
                        "Expected group {:?} not found in results", expected_group);
            
            let actual_values = &actual_results[expected_group];
            
            // Property 18e: COUNT values should match exactly
            prop_assert_eq!(actual_values[0], expected_values[0],
                           "COUNT mismatch for group {:?}: expected {}, got {}", 
                           expected_group, expected_values[0], actual_values[0]);
            
            // Property 18f: SUM values should match exactly
            prop_assert_eq!(actual_values[1], expected_values[1],
                           "SUM mismatch for group {:?}: expected {}, got {}", 
                           expected_group, expected_values[1], actual_values[1]);
        }
        
        // Property 18g: No extra groups should be present in results
        prop_assert_eq!(actual_results.len(), expected_results.len(),
                       "Result contains unexpected groups");
        
        // Property 18h: Total count across all groups should equal input row count
        let total_input_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let total_count: f64 = actual_results.values().map(|values| values[0]).sum();
        prop_assert_eq!(total_count as usize, total_input_rows,
                       "Total count across groups doesn't match input row count");
        
        // Property 18i: Total sum across all groups should equal sum of all input values
        let total_input_sum: i64 = batches.iter().flat_map(|batch| {
            let (col_idx, _) = batch.schema().column_with_name(&numeric_columns[0]).unwrap();
            let int_array = batch.column(col_idx).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..batch.num_rows()).map(|i| int_array.value(i))
        }).sum();
        
        let total_result_sum: f64 = actual_results.values().map(|values| values[1]).sum();
        prop_assert_eq!(total_result_sum as i64, total_input_sum,
                       "Total sum across groups doesn't match input sum");
    }
    
    /// Test GROUP BY correctness with multiple batches to ensure cross-batch aggregation works
    #[test]
    fn test_group_by_correctness_multiple_batches(
        batches in arb_groupby_record_batches(),
    ) {
        // This test ensures that GROUP BY works correctly when the same groups 
        // appear across multiple input batches
        prop_assume!(batches.len() >= 2);
        prop_assume!(!batches.is_empty());
        prop_assume!(batches[0].num_rows() > 0);
        prop_assume!(batches[0].num_columns() >= 2);
        
        let schema = batches[0].schema();
        
        // Use first string column for grouping
        let group_by_columns: Vec<String> = schema
            .fields()
            .iter()
            .filter(|field| field.data_type() == &DataType::Utf8)
            .map(|field| field.name().clone())
            .take(1)
            .collect();
        
        // Use first numeric column for aggregation
        let numeric_columns: Vec<String> = schema
            .fields()
            .iter()
            .filter(|field| field.data_type() == &DataType::Int64)
            .map(|field| field.name().clone())
            .take(1)
            .collect();
        
        prop_assume!(!group_by_columns.is_empty());
        prop_assume!(!numeric_columns.is_empty());
        
        let aggregates = vec![
            Aggregate {
                function: AggregateFunction::Count,
                column: "*".to_string(),
                alias: Some("count_star".to_string()),
            },
            Aggregate {
                function: AggregateFunction::Sum,
                column: numeric_columns[0].clone(),
                alias: Some(format!("sum_{}", numeric_columns[0])),
            },
        ];
        
        // Test that processing all batches together gives same result as 
        // processing them separately and then combining
        let executor = TaskExecutor::new();
        
        // Process all batches together
        let combined_result = executor.perform_group_by_aggregation(&batches, &group_by_columns, &aggregates);
        prop_assert!(combined_result.is_ok(), "Combined GROUP BY failed: {:?}", combined_result.err());
        
        let combined_batch = combined_result.unwrap();
        let combined_results = extract_actual_results(&combined_batch, &group_by_columns, &aggregates);
        
        // Process each batch separately and manually combine
        let mut manual_groups: HashMap<Vec<String>, Vec<f64>> = HashMap::new();
        
        for batch in &batches {
            let single_batch_result = executor.perform_group_by_aggregation(&[batch.clone()], &group_by_columns, &aggregates);
            prop_assert!(single_batch_result.is_ok(), "Single batch GROUP BY failed");
            
            let single_result_batch = single_batch_result.unwrap();
            let single_results = extract_actual_results(&single_result_batch, &group_by_columns, &aggregates);
            
            // Manually combine results
            for (group_key, values) in single_results {
                let entry = manual_groups.entry(group_key).or_insert_with(|| vec![0.0; aggregates.len()]);
                entry[0] += values[0]; // COUNT
                entry[1] += values[1]; // SUM
            }
        }
        
        // Property: Combined processing should give same results as manual combination
        prop_assert_eq!(combined_results.len(), manual_groups.len(),
                       "Different number of groups between combined and manual processing");
        
        for (group_key, expected_values) in &manual_groups {
            prop_assert!(combined_results.contains_key(group_key),
                        "Group {:?} missing from combined results", group_key);
            
            let actual_values = &combined_results[group_key];
            prop_assert_eq!(actual_values[0], expected_values[0],
                           "COUNT mismatch for group {:?} in multi-batch processing", group_key);
            prop_assert_eq!(actual_values[1], expected_values[1],
                           "SUM mismatch for group {:?} in multi-batch processing", group_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_groupby_generators() {
        // Test that our generators produce valid data for GROUP BY testing
        let schema = arb_groupby_schema().new_tree(&mut proptest::test_runner::TestRunner::default()).unwrap().current();
        
        // Should have at least one field
        assert!(!schema.fields().is_empty());
        
        // Should have both string and numeric fields
        let has_string = schema.fields().iter().any(|f| f.data_type() == &DataType::Utf8);
        let has_numeric = schema.fields().iter().any(|f| f.data_type() == &DataType::Int64);
        assert!(has_string, "Schema should have string fields for grouping");
        assert!(has_numeric, "Schema should have numeric fields for aggregation");
        
        let schema_arc = Arc::new(schema);
        let batch = arb_groupby_record_batch(schema_arc).new_tree(&mut proptest::test_runner::TestRunner::default()).unwrap().current();
        
        assert!(batch.num_rows() > 0);
        assert!(batch.num_columns() >= 2);
    }
    
    #[test]
    fn test_expected_results_calculation() {
        // Test the manual calculation function with known data
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));

        let category_array = StringArray::from(vec!["A", "B", "A", "A", "B"]);
        let amount_array = Int64Array::from(vec![10, 20, 15, 5, 25]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(category_array), Arc::new(amount_array)],
        ).unwrap();

        let aggregates = vec![
            Aggregate {
                function: AggregateFunction::Count,
                column: "*".to_string(),
                alias: Some("count".to_string()),
            },
            Aggregate {
                function: AggregateFunction::Sum,
                column: "amount".to_string(),
                alias: Some("sum_amount".to_string()),
            },
        ];

        let results = calculate_expected_results(&[batch], &["category".to_string()], &aggregates);
        
        // Should have 2 groups: A and B
        assert_eq!(results.len(), 2);
        
        // Group A: count=3, sum=30 (10+15+5)
        let group_a = vec!["A".to_string()];
        assert!(results.contains_key(&group_a));
        assert_eq!(results[&group_a][0], 3.0); // count
        assert_eq!(results[&group_a][1], 30.0); // sum
        
        // Group B: count=2, sum=45 (20+25)
        let group_b = vec!["B".to_string()];
        assert!(results.contains_key(&group_b));
        assert_eq!(results[&group_b][0], 2.0); // count
        assert_eq!(results[&group_b][1], 45.0); // sum
    }
}