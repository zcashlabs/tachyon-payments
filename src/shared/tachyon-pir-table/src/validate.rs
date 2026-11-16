//! Table validation: every row through `tachyon_pir_protocol::validate_row`
//! (spec 6.2), and the table rules of spec 6.3, 6.4 and 10.3.
//!
//! The publisher's verify phase runs [`validate_scope`] on what it built; a
//! query host runs [`validate_published`] on rows it fetched, against the map
//! table that names them.

use rayon::prelude::*;
use tachyon_pir_protocol::map::ScopeTables as MapScope;
use tachyon_pir_protocol::{
    BlockHash, MAX_SEEDS, MAX_SEGMENTS, Product, ROW_BYTES, RowContext, Table, n_buckets_for,
    validate_row,
};

use crate::build::ScopeTables;
use crate::digest::rows_sha256;
use crate::error::Error;

/// Validates every row of a table and returns its entry count.
///
/// `template` is the context of the table with any `row`; each row is
/// validated with `row` set to its index. `rows` must be exactly
/// `template.rows x 8192` bytes. On failure, reports the lowest bad row.
pub fn validate_table(product: Product, template: &RowContext, rows: &[u8]) -> Result<u64, Error> {
    let expected = template.rows as usize * ROW_BYTES;
    if rows.len() != expected {
        return Err(Error::RowsLength {
            expected,
            actual: rows.len(),
        });
    }
    let counts: Vec<_> = rows
        .par_chunks(ROW_BYTES)
        .enumerate()
        .map(|(row, bytes)| {
            let context = RowContext {
                row: row as u32,
                ..*template
            };
            validate_row(product, bytes, &context).map(|entries| entries.len() as u64)
        })
        .collect();
    let mut total = 0;
    for (row, count) in counts.into_iter().enumerate() {
        total += count.map_err(|source| Error::InvalidRow {
            row: row as u32,
            source,
        })?;
    }
    Ok(total)
}

/// Validates rows fetched for `table` of map scope `scope`: their digest
/// against `rows_sha256`, every row under the context the map implies, and
/// the entry count against `entries`. Returns the entry count.
pub fn validate_published<S: MapScope>(
    product: Product,
    genesis: &BlockHash,
    scope: &S,
    table: &Table,
    rows: &[u8],
) -> Result<u64, Error> {
    let expected = table.geometry.table_bytes();
    if rows.len() != expected {
        return Err(Error::RowsLength {
            expected,
            actual: rows.len(),
        });
    }
    if rows_sha256(rows) != table.rows_sha256.0 {
        return Err(Error::RowsDigest);
    }
    let template = scope.row_context(product, genesis, table, 0);
    let actual = validate_table(product, &template, rows)?;
    if actual != table.entries {
        return Err(Error::EntryCount {
            expected: table.entries,
            actual,
        });
    }
    Ok(actual)
}

/// Validates a built scope: the table rules (ascending `(bucket, segment)`,
/// every bucket with segments `0..k`, `1 <= k <= MAX_SEGMENTS`, equal
/// segment-0 seeds, a nonzero scope seed only with single segments, seeds
/// below `MAX_SEEDS`, nonempty overflow segments, `n_buckets` as
/// `n_buckets_for` the total), each table's digest, and every row of every
/// table with its entry count. `heights` is the scope's
/// `[first_height, last_height]`, required for tachygram. Returns the total
/// entry count.
pub fn validate_scope(
    product: Product,
    genesis: &BlockHash,
    scope_id: u64,
    heights: Option<(u32, u32)>,
    scope: &ScopeTables,
) -> Result<u64, Error> {
    let layout = |reason: String| Err(Error::Layout(reason));
    if scope.tables.is_empty() {
        if scope.n_buckets != 1 {
            return layout("an empty scope has one bucket".into());
        }
        return Ok(0);
    }
    let in_table = |table: &crate::BuiltTable, source: Error| Error::InTable {
        bucket: table.bucket,
        segment: table.segment,
        source: Box::new(source),
    };
    if scope
        .tables
        .windows(2)
        .any(|w| (w[0].bucket, w[0].segment) >= (w[1].bucket, w[1].segment))
    {
        return layout("tables are not in ascending (bucket, segment) order".into());
    }
    if scope
        .tables
        .last()
        .is_some_and(|t| t.bucket >= scope.n_buckets)
    {
        return layout("a table's bucket is out of range".into());
    }
    let geometry = scope.tables[0].geometry;
    let mut total = 0u64;
    for bucket in 0..scope.n_buckets {
        let segments = scope.segments(bucket);
        if segments.is_empty() || segments.len() > MAX_SEGMENTS as usize {
            return layout(format!("bucket {bucket} has {} segments", segments.len()));
        }
        if scope.scope_seed != 0 && segments.len() != 1 {
            return layout("a nonzero scope seed allows no overflow segments".into());
        }
        for (k, table) in segments.iter().enumerate() {
            if table.segment as usize != k {
                return layout(format!("bucket {bucket}: segments not contiguous from 0"));
            }
            if table.geometry != geometry {
                return layout("tables of one scope have different geometries".into());
            }
            if table.seed >= MAX_SEEDS {
                return layout(format!("seed {} is out of range", table.seed));
            }
            if k == 0 && table.seed != scope.scope_seed {
                return layout(format!(
                    "bucket {bucket}: segment 0 seed is not the scope seed"
                ));
            }
            if k > 0 && table.entries == 0 {
                return layout(format!("bucket {bucket}: empty overflow segment {k}"));
            }
            if table.rows.len() != geometry.table_bytes() {
                return Err(in_table(
                    table,
                    Error::RowsLength {
                        expected: geometry.table_bytes(),
                        actual: table.rows.len(),
                    },
                ));
            }
            if rows_sha256(&table.rows) != table.rows_sha256 {
                return Err(in_table(table, Error::RowsDigest));
            }
            let template = scope.row_context(product, genesis, scope_id, table, heights);
            let count = validate_table(product, &template, &table.rows)
                .map_err(|source| in_table(table, source))?;
            if count != u64::from(table.entries) {
                return Err(in_table(
                    table,
                    Error::EntryCount {
                        expected: u64::from(table.entries),
                        actual: count,
                    },
                ));
            }
            total += count;
        }
    }
    if n_buckets_for(product, geometry, total) != scope.n_buckets {
        return layout(format!(
            "n_buckets {} is not n_buckets_for({total})",
            scope.n_buckets
        ));
    }
    Ok(total)
}
