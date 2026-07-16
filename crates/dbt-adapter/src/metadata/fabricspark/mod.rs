//! Metadata queries for Microsoft Fabric Lakehouse (Spark via Livy).
//!
//! Fabric Spark has no `system.information_schema` (unlike Databricks Unity
//! Catalog), so relation listing is based on `SHOW TABLE EXTENDED`, which
//! reports the relation type and storage provider for every relation in a
//! schema in one query. This works for both classic lakehouses (2-part
//! `schema.table` naming) and schema-enabled lakehouses (3-part
//! `lakehouse.schema.table`, with the lakehouse in the catalog slot).

use std::sync::Arc;

use arrow_array::*;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::BaseRelation;

use crate::AdapterEngine;
use crate::errors::AdapterResult;
use crate::metadata::CatalogAndSchema;
use crate::record_batch::RecordBatchExt;
use crate::relation::Relation;
use dbt_adbc::{Connection, QueryCtx};

/// Extracts the value of a `Key: value` line from `SHOW TABLE EXTENDED`'s
/// `information` column.
fn information_value<'a>(information: &'a str, key: &str) -> Option<&'a str> {
    information.lines().find_map(|line| {
        line.strip_prefix(key)
            .and_then(|rest| rest.strip_prefix(": "))
            .map(str::trim)
    })
}

pub fn list_relations(
    engine: &dyn AdapterEngine,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &CatalogAndSchema,
    token: CancellationToken,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    let catalog = &db_schema.resolved_catalog;
    let schema = &db_schema.resolved_schema;
    let namespace = if catalog.is_empty() {
        format!("`{schema}`")
    } else {
        format!("`{catalog}`.`{schema}`")
    };

    let sql = format!("SHOW TABLE EXTENDED IN {namespace} LIKE '*'");
    let batch = match engine.execute(None, conn, ctx, &sql, token) {
        // A schema that doesn't exist yet has no relations; dbt will create it.
        Err(e) if e.to_string().contains("SCHEMA_NOT_FOUND") => return Ok(Vec::new()),
        other => other?,
    };

    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let names = batch.column_values::<StringArray>("tableName")?;
    let is_temporary = batch.column_values::<BooleanArray>("isTemporary")?;
    let information = batch.column_values::<StringArray>("information")?;

    let mut relations = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        if is_temporary.value(i) {
            continue;
        }
        let info = information.value(i);
        // `Type:` is MANAGED, EXTERNAL or VIEW; `Provider:` is the storage
        // format (`delta` for regular lakehouse tables) and absent for views.
        let table_type = information_value(info, "Type").unwrap_or("MANAGED");
        let is_delta = information_value(info, "Provider") == Some("delta");

        let relation = Arc::new(
            Relation::new(
                engine.adapter_type(),
                (!catalog.is_empty()).then(|| catalog.to_string()),
                schema.to_string(),
                names.value(i).to_string(),
            )
            .with_relation_type(RelationType::from_adapter_type(
                engine.adapter_type(),
                table_type,
            ))
            .with_quoting(engine.quoting())
            .with_is_delta(is_delta),
        ) as Arc<dyn BaseRelation>;
        relations.push(relation);
    }

    Ok(relations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn information_value_parses_show_table_extended_lines() {
        let info = "Catalog: spark_catalog\nDatabase: `WS`.`lh`.`dbo`\nTable: probe\nType: MANAGED\nProvider: delta\nLocation: abfss://...\n";
        assert_eq!(information_value(info, "Type"), Some("MANAGED"));
        assert_eq!(information_value(info, "Provider"), Some("delta"));
        assert_eq!(information_value(info, "Missing"), None);

        let view_info = "Catalog: spark_catalog\nTable: v\nType: VIEW\nView Text: SELECT 1\n";
        assert_eq!(information_value(view_info, "Type"), Some("VIEW"));
        assert_eq!(information_value(view_info, "Provider"), None);
    }
}
