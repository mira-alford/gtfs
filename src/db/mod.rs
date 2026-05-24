//! DB
//!
//! DB module exposes the Db struct,
//! which abstracts away all the dirty Db operations.

pub mod queries;
mod tests;
pub mod types;

use crate::vars;
use anyhow::Result;
use sqlx::PgPool;
use tracing::{info, instrument};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Db struct wraps the Db and provides a wonderful interface.
#[derive(Debug, Clone)]
pub struct Db(pub PgPool);

impl Db {
    /// Attempt to connect to the database, using DATABASE_URL.
    #[instrument]
    pub async fn connect() -> Result<Db> {
        info!("Attempting to connect to db");
        let db_url = vars::db_url();
        let pool = PgPool::connect(&db_url).await?;
        info!("Connected to db");
        Ok(Db(pool))
    }

    /// Run the migrations on the database, using MIGRATOR
    #[instrument(name = "db_migrations", skip(self))]
    pub async fn run_migrations(&mut self) -> Result<()> {
        info!("Attempting DB migrations");
        MIGRATOR.run(&self.0).await?;
        info!("Db migrations complete");
        Ok(())
    }
}
