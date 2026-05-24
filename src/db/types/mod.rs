//! DB Types
//!
//! Types for database operations.
//! Directly map to schema tables.
pub mod static_types;

use chrono::{NaiveDate, NaiveDateTime, Timelike, Utc};
use sqlx::{
    FromRow, PgConnection, Postgres, QueryBuilder, Transaction, postgres::types::PgInterval,
    query_builder::Separated,
};

use anyhow::Result;

pub trait InsertDB: Sized + Send + Sync {
    fn insert_into(qb: &mut QueryBuilder<Postgres>);
    fn value(self, qb: &mut Separated<Postgres, &'static str>);
}

/// Generic insert helper
pub async fn insert_one<T: InsertDB>(item: T, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let mut qb = QueryBuilder::<Postgres>::new("");

    T::insert_into(&mut qb);

    qb.push(" VALUES (");
    item.value(&mut qb.separated(","));
    qb.push(")");

    qb.build().execute(&mut **tx).await?;

    Ok(())
}

/// Generic bulk insert helper
pub async fn insert_many<T: InsertDB>(
    items: Vec<T>,
    tx: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }

    let mut qb = QueryBuilder::<Postgres>::new("");

    T::insert_into(&mut qb);

    qb.push_values(items.into_iter(), |mut b, item| {
        item.value(&mut b);
    });

    qb.build().execute(&mut **tx).await?;

    Ok(())
}
