use std::future::Future;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use crate::error::Error;
use crate::executor::Executor;
use crate::pool::{Pool, PoolOptions};
use crate::query::query;
use crate::query_as::query_as;
use crate::{MySql, MySqlConnectOptions, MySqlConnection, MySqlDatabaseError};
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_core::query_builder::QueryBuilder;
use sqlx_core::query_scalar::query_scalar;
use sqlx_core::sql_str::AssertSqlSafe;
use sqlx_core::testing::{migrations_hash, template_db_name};

pub(crate) use sqlx_core::testing::*;

// Using a blocking `OnceLock` here because the critical sections are short.
static MASTER_POOL: OnceLock<Pool<MySql>> = OnceLock::new();

/// Environment variable to enable template cloning.
const SQLX_TEST_TEMPLATE: &str = "SQLX_TEST_TEMPLATE";

/// Lock name for template database synchronization.
const TEMPLATE_LOCK_NAME: &str = "sqlx_template_lock";

/// Check if template cloning is enabled.
fn templates_enabled() -> bool {
    std::env::var(SQLX_TEST_TEMPLATE).is_ok()
}

/// Guard for MySQL's GET_LOCK/RELEASE_LOCK to ensure locks are released.
///
/// MySQL's GET_LOCK is session-based, so the lock is tied to the connection that
/// acquired it. The lock will be automatically released when the connection is closed.
/// This guard tracks whether explicit release was called and warns if not.
struct TemplateLockGuard {
    released: bool,
}

impl TemplateLockGuard {
    /// Acquire the template lock with a 300 second timeout.
    async fn acquire(conn: &mut MySqlConnection) -> Result<Self, Error> {
        let lock_result: Option<i32> = query_scalar("SELECT GET_LOCK(?, 300)")
            .bind(TEMPLATE_LOCK_NAME)
            .fetch_one(&mut *conn)
            .await?;

        if lock_result != Some(1) {
            return Err(Error::Protocol(format!(
                "Failed to acquire template lock, GET_LOCK returned: {:?}",
                lock_result
            )));
        }

        Ok(Self { released: false })
    }

    /// Release the template lock.
    async fn release(mut self, conn: &mut MySqlConnection) -> Result<(), Error> {
        query("SELECT RELEASE_LOCK(?)")
            .bind(TEMPLATE_LOCK_NAME)
            .execute(&mut *conn)
            .await?;
        self.released = true;
        Ok(())
    }

    /// Release the lock, ignoring errors (for cleanup paths).
    async fn release_ignore_errors(mut self, conn: &mut MySqlConnection) {
        let _ = query("SELECT RELEASE_LOCK(?)")
            .bind(TEMPLATE_LOCK_NAME)
            .execute(&mut *conn)
            .await;
        self.released = true;
    }
}

impl Drop for TemplateLockGuard {
    fn drop(&mut self) {
        if !self.released {
            // Lock will be released when the connection is closed.
            // This warning helps identify code paths that skip explicit release.
            eprintln!("warning: TemplateLockGuard dropped without explicit release");
        }
    }
}

/// Get or create a template database with migrations applied.
/// Returns the template database name if successful, or None if templates are disabled.
async fn get_or_create_template(
    conn: &mut MySqlConnection,
    master_opts: &MySqlConnectOptions,
    migrator: &sqlx_core::migrate::Migrator,
) -> Result<Option<String>, Error> {
    if !templates_enabled() {
        return Ok(None);
    }

    let hash = migrations_hash(migrator);
    let tpl_name = template_db_name(&hash);

    // Acquire lock for template creation/access
    let lock = TemplateLockGuard::acquire(conn).await?;

    // Ensure template tracking table exists
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS _sqlx_test_templates (
            template_name VARCHAR(255) PRIMARY KEY,
            migrations_hash VARCHAR(64) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_used_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY (migrations_hash)
        )
        "#,
    )
    .await?;

    // Check if template already exists in tracking table
    let existing: Option<String> =
        query_scalar("SELECT template_name FROM _sqlx_test_templates WHERE migrations_hash = ?")
            .bind(&hash)
            .fetch_optional(&mut *conn)
            .await?;

    if let Some(existing_name) = existing {
        // Template exists, update last_used_at and return
        query("UPDATE _sqlx_test_templates SET last_used_at = CURRENT_TIMESTAMP WHERE template_name = ?")
            .bind(&existing_name)
            .execute(&mut *conn)
            .await?;

        lock.release(conn).await?;
        return Ok(Some(existing_name));
    }

    // Create new template database (use IF NOT EXISTS for idempotency)
    // The database might exist from a previous run without being registered
    conn.execute(AssertSqlSafe(format!(
        "CREATE DATABASE IF NOT EXISTS `{tpl_name}`"
    )))
    .await?;

    // Check if this is a fresh database or one left over from a previous run
    // by checking if it already has migrations recorded
    let template_opts = master_opts.clone().database(&tpl_name);
    let mut template_conn: MySqlConnection = template_opts.connect().await?;

    // Try to count migrations - if the table doesn't exist or is empty, we need to run migrations
    let migration_count: Result<i64, _> = query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&mut template_conn)
        .await;

    let needs_migrations = match &migration_count {
        Ok(count) => {
            eprintln!("template {tpl_name}: found {} existing migrations", count);
            *count == 0
        }
        Err(e) => {
            eprintln!(
                "template {tpl_name}: migration check error (table may not exist): {}",
                e
            );
            true
        }
    };

    // Only run migrations if the database is fresh (no migrations table)
    if needs_migrations {
        if let Err(e) = migrator.run_direct(None, &mut template_conn).await {
            // Clean up on failure
            template_conn.close().await.ok();
            conn.execute(AssertSqlSafe(format!(
                "DROP DATABASE IF EXISTS `{tpl_name}`"
            )))
            .await
            .ok();
            lock.release_ignore_errors(conn).await;
            return Err(Error::Protocol(format!(
                "Failed to apply migrations to template: {}",
                e
            )));
        }
    }

    template_conn.close().await?;

    // Register template (use INSERT IGNORE in case it was already registered by another process)
    query("INSERT IGNORE INTO _sqlx_test_templates (template_name, migrations_hash) VALUES (?, ?)")
        .bind(&tpl_name)
        .bind(&hash)
        .execute(&mut *conn)
        .await?;

    lock.release(conn).await?;

    eprintln!("created template database {tpl_name}");

    Ok(Some(tpl_name))
}

/// Clone a template database to a new test database.
async fn clone_database(
    conn: &mut MySqlConnection,
    master_opts: &MySqlConnectOptions,
    template_name: &str,
    new_db_name: &str,
) -> Result<(), Error> {
    // First, create the new empty database
    conn.execute(AssertSqlSafe(format!("CREATE DATABASE `{new_db_name}`")))
        .await?;

    // Clone all database objects in-process
    clone_database_objects(conn, master_opts, template_name, new_db_name).await
}

/// Format a comma-separated column list with backticks.
fn format_column_list(columns: &str) -> String {
    columns
        .split(',')
        .map(|c| format!("`{}`", c.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format a foreign key action clause.
fn format_fk_action(rule: &str) -> String {
    match rule {
        "NO ACTION" => String::new(), // Default, no need to specify
        "RESTRICT" => "ON DELETE RESTRICT".to_string(),
        "CASCADE" => "ON DELETE CASCADE".to_string(),
        "SET NULL" => "ON DELETE SET NULL".to_string(),
        "SET DEFAULT" => "ON DELETE SET DEFAULT".to_string(),
        _ => String::new(),
    }
}

/// Format a foreign key ON UPDATE action clause.
fn format_fk_update_action(rule: &str) -> String {
    match rule {
        "NO ACTION" => String::new(), // Default, no need to specify
        "RESTRICT" => "ON UPDATE RESTRICT".to_string(),
        "CASCADE" => "ON UPDATE CASCADE".to_string(),
        "SET NULL" => "ON UPDATE SET NULL".to_string(),
        "SET DEFAULT" => "ON UPDATE SET DEFAULT".to_string(),
        _ => String::new(),
    }
}

/// Clone all database objects from template to new database using in-process SQL commands.
/// Copies: tables (with data), foreign keys, views, triggers, routines (procedures/functions), and events.
async fn clone_database_objects(
    conn: &mut MySqlConnection,
    master_opts: &MySqlConnectOptions,
    template_name: &str,
    new_db_name: &str,
) -> Result<(), Error> {
    // Connect to the new database for copying
    let new_db_opts = master_opts.clone().database(new_db_name);
    let mut new_conn: MySqlConnection = new_db_opts.connect().await?;

    // 1. Copy tables (structure + data)
    let tables: Vec<String> = query_scalar(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
    )
    .bind(template_name)
    .fetch_all(&mut *conn)
    .await?;

    for table in &tables {
        // Copy table structure (indexes, defaults, but NOT foreign keys - LIKE doesn't copy FKs)
        new_conn
            .execute(AssertSqlSafe(format!(
                "CREATE TABLE `{new_db_name}`.`{table}` LIKE `{template_name}`.`{table}`"
            )))
            .await?;

        // Copy table data (for migrations table, etc.)
        new_conn
            .execute(AssertSqlSafe(format!(
                "INSERT INTO `{new_db_name}`.`{table}` SELECT * FROM `{template_name}`.`{table}`"
            )))
            .await?;
    }

    // Fix AUTO_INCREMENT values after copying data
    // CREATE TABLE ... LIKE doesn't preserve the current AUTO_INCREMENT value, so after
    // copying data we need to reset it to MAX(pk) + 1 to avoid duplicate key errors.
    let auto_inc_tables: Vec<(String, String)> = query_as(
        r#"
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = ?
            AND extra LIKE '%auto_increment%'
        "#,
    )
    .bind(new_db_name)
    .fetch_all(&mut new_conn)
    .await?;

    for (table_name, column_name) in &auto_inc_tables {
        // Get the maximum value in the auto_increment column
        let max_val: Option<i64> = query_scalar(AssertSqlSafe(format!(
            "SELECT MAX(`{column_name}`) FROM `{new_db_name}`.`{table_name}`"
        )))
        .fetch_one(&mut new_conn)
        .await?;

        // Set AUTO_INCREMENT to max + 1 (or 1 if table is empty)
        let next_val = max_val.unwrap_or(0) + 1;
        new_conn
            .execute(AssertSqlSafe(format!(
                "ALTER TABLE `{new_db_name}`.`{table_name}` AUTO_INCREMENT = {next_val}"
            )))
            .await?;
    }

    // Copy foreign key constraints (CREATE TABLE ... LIKE doesn't copy these)
    // Query all FKs, grouping columns for composite keys
    // Returns: (constraint_name, table_name, columns, referenced_table_name, ref_columns, delete_rule, update_rule)
    let foreign_keys: Vec<(String, String, String, String, String, String, String)> = query_as(
        r#"
        SELECT
            tc.constraint_name,
            kcu.table_name,
            GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
            kcu.referenced_table_name,
            GROUP_CONCAT(kcu.referenced_column_name ORDER BY kcu.ordinal_position) as ref_columns,
            rc.delete_rule,
            rc.update_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.table_schema = ?
            AND tc.constraint_type = 'FOREIGN KEY'
        GROUP BY tc.constraint_name, kcu.table_name, kcu.referenced_table_name,
                 rc.delete_rule, rc.update_rule
        "#,
    )
    .bind(template_name)
    .fetch_all(&mut *conn)
    .await?;

    for (constraint_name, table_name, fk_columns, ref_table, ref_cols, delete_rule, update_rule) in
        &foreign_keys
    {
        let columns = format_column_list(fk_columns);
        let ref_columns = format_column_list(ref_cols);
        let on_delete = format_fk_action(delete_rule);
        let on_update = format_fk_update_action(update_rule);

        new_conn
            .execute(AssertSqlSafe(format!(
                "ALTER TABLE `{new_db_name}`.`{table_name}` ADD CONSTRAINT `{constraint_name}` \
                 FOREIGN KEY ({columns}) REFERENCES `{ref_table}`({ref_columns}) {on_delete} {on_update}"
            )))
            .await?;
    }

    // 2. Copy views
    // Views may depend on other views, so we need to handle dependency order.
    // We use a retry loop: try to create all views, collect failures, retry until
    // all succeed or no progress is made (which indicates a circular dependency or other error).
    let views: Vec<String> = query_scalar(
        "SELECT table_name FROM information_schema.views WHERE table_schema = ?",
    )
    .bind(template_name)
    .fetch_all(&mut *conn)
    .await?;

    // Fetch CREATE VIEW statements for all views upfront
    let mut view_stmts: Vec<(String, String)> = Vec::with_capacity(views.len());
    for view in &views {
        // SHOW CREATE VIEW returns: View, Create View, character_set_client, collation_connection
        let row: (String, String, String, String) = query_as(AssertSqlSafe(format!(
            "SHOW CREATE VIEW `{template_name}`.`{view}`"
        )))
        .fetch_one(&mut *conn)
        .await?;

        let create_stmt = replace_database_refs(&row.1, template_name, new_db_name);
        view_stmts.push((view.clone(), create_stmt));
    }

    // Retry loop for view creation (handles dependency order)
    let mut pending_views = view_stmts;
    let max_attempts = pending_views.len() + 1; // At most N iterations for N views
    for _ in 0..max_attempts {
        if pending_views.is_empty() {
            break;
        }

        let prev_count = pending_views.len();
        let mut failed_views = Vec::new();
        for (view_name, create_stmt) in pending_views {
            match new_conn.execute(AssertSqlSafe(create_stmt.clone())).await {
                Ok(_) => {} // Success, view created
                Err(_) => {
                    // Failed, likely due to missing dependency - will retry
                    failed_views.push((view_name, create_stmt));
                }
            }
        }

        // If no progress was made (same number of failures), we have an unresolvable error
        if failed_views.len() == prev_count && !failed_views.is_empty() {
            // Try one more time and return the actual error
            let (_view_name, create_stmt) = failed_views.into_iter().next().unwrap();
            return Err(new_conn
                .execute(AssertSqlSafe(create_stmt))
                .await
                .unwrap_err());
        }

        pending_views = failed_views;
    }

    // 3. Copy triggers
    let triggers: Vec<String> = query_scalar(
        "SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema = ?",
    )
    .bind(template_name)
    .fetch_all(&mut *conn)
    .await?;

    for trigger in &triggers {
        // SHOW CREATE TRIGGER returns: Trigger, sql_mode, SQL Original Statement, ...
        let row: (String, String, String, String, String, String, String) = query_as(
            AssertSqlSafe(format!("SHOW CREATE TRIGGER `{template_name}`.`{trigger}`")),
        )
        .fetch_one(&mut *conn)
        .await?;

        // The CREATE TRIGGER statement is in column 2 (index 2)
        let create_stmt = replace_database_refs(&row.2, template_name, new_db_name);
        new_conn.execute(AssertSqlSafe(create_stmt)).await?;
    }

    // 4. Copy routines (procedures and functions)
    let routines: Vec<(String, String)> = query_as(
        "SELECT routine_name, routine_type FROM information_schema.routines WHERE routine_schema = ?",
    )
    .bind(template_name)
    .fetch_all(&mut *conn)
    .await?;

    for (routine_name, routine_type) in &routines {
        let show_cmd = if routine_type == "PROCEDURE" {
            format!("SHOW CREATE PROCEDURE `{template_name}`.`{routine_name}`")
        } else {
            format!("SHOW CREATE FUNCTION `{template_name}`.`{routine_name}`")
        };

        // SHOW CREATE PROCEDURE/FUNCTION returns: Name, sql_mode, Create ..., ...
        let row: (String, String, String, String, String, String) =
            query_as(AssertSqlSafe(show_cmd))
                .fetch_one(&mut *conn)
                .await?;

        // The CREATE statement is in column 2 (index 2)
        let create_stmt = replace_database_refs(&row.2, template_name, new_db_name);
        new_conn.execute(AssertSqlSafe(create_stmt)).await?;
    }

    // 5. Copy events
    let events: Vec<String> = query_scalar(
        "SELECT event_name FROM information_schema.events WHERE event_schema = ?",
    )
    .bind(template_name)
    .fetch_all(&mut *conn)
    .await?;

    for event in &events {
        // SHOW CREATE EVENT returns: Event, sql_mode, time_zone, Create Event, ...
        let row: (String, String, String, String, String, String) = query_as(AssertSqlSafe(
            format!("SHOW CREATE EVENT `{template_name}`.`{event}`"),
        ))
        .fetch_one(&mut *conn)
        .await?;

        // The CREATE EVENT statement is in column 3 (index 3)
        let create_stmt = replace_database_refs(&row.3, template_name, new_db_name);
        new_conn.execute(AssertSqlSafe(create_stmt)).await?;
    }

    new_conn.close().await?;

    Ok(())
}

impl TestSupport for MySql {
    fn test_context(
        args: &TestArgs,
    ) -> impl Future<Output = Result<TestContext<Self>, Error>> + Send + '_ {
        test_context(args)
    }

    async fn cleanup_test(db_name: &str) -> Result<(), Error> {
        let mut conn = MASTER_POOL
            .get()
            .expect("cleanup_test() invoked outside `#[sqlx::test]`")
            .acquire()
            .await?;

        do_cleanup(&mut conn, db_name).await
    }

    async fn cleanup_test_dbs() -> Result<Option<usize>, Error> {
        let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");

        let mut conn = MySqlConnection::connect(&url).await?;

        let delete_db_names: Vec<String> = query_scalar("select db_name from _sqlx_test_databases")
            .fetch_all(&mut conn)
            .await?;

        if delete_db_names.is_empty() {
            return Ok(None);
        }

        let mut deleted_db_names = Vec::with_capacity(delete_db_names.len());

        let mut builder = QueryBuilder::new("drop database if exists ");

        for db_name in &delete_db_names {
            builder.push(db_name);

            match builder.build().execute(&mut conn).await {
                Ok(_deleted) => {
                    deleted_db_names.push(db_name);
                }
                // Assume a database error just means the DB is still in use.
                Err(Error::Database(dbe)) => {
                    eprintln!("could not clean test database {db_name:?}: {dbe}")
                }
                // Bubble up other errors
                Err(e) => return Err(e),
            }

            builder.reset();
        }

        if deleted_db_names.is_empty() {
            return Ok(None);
        }

        let mut query = QueryBuilder::new("delete from _sqlx_test_databases where db_name in (");

        let mut separated = query.separated(",");

        for db_name in &deleted_db_names {
            separated.push_bind(db_name);
        }

        query.push(")").build().execute(&mut conn).await?;

        let _ = conn.close().await;
        Ok(Some(delete_db_names.len()))
    }

    async fn snapshot(_conn: &mut Self::Connection) -> Result<FixtureSnapshot<Self>, Error> {
        // TODO: I want to get the testing feature out the door so this will have to wait,
        // but I'm keeping the code around for now because I plan to come back to it.
        todo!()
    }
}

async fn test_context(args: &TestArgs) -> Result<TestContext<MySql>, Error> {
    let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let master_opts = MySqlConnectOptions::from_str(&url).expect("failed to parse DATABASE_URL");

    let pool = PoolOptions::new()
        // MySql's normal connection limit is 150 plus 1 superuser connection
        // We don't want to use the whole cap and there may be fuzziness here due to
        // concurrently running tests anyway.
        .max_connections(20)
        // Immediately close master connections. Tokio's I/O streams don't like hopping runtimes.
        .after_release(|_conn, _| Box::pin(async move { Ok(false) }))
        .connect_lazy_with(master_opts.clone());

    let master_pool = match once_lock_try_insert_polyfill(&MASTER_POOL, pool) {
        Ok(inserted) => inserted,
        Err((existing, pool)) => {
            // Sanity checks.
            assert_eq!(
                existing.connect_options().host,
                pool.connect_options().host,
                "DATABASE_URL changed at runtime, host differs"
            );

            assert_eq!(
                existing.connect_options().database,
                pool.connect_options().database,
                "DATABASE_URL changed at runtime, database differs"
            );

            existing
        }
    };

    let mut conn = master_pool.acquire().await?;

    cleanup_old_dbs(&mut conn).await?;

    // language=MySQL
    conn.execute(
        r#"
        create table if not exists _sqlx_test_databases (
            db_name text not null,
            test_path text not null,
            created_at timestamp not null default current_timestamp,
            -- BLOB/TEXT columns can only be used as index keys with a prefix length:
            -- https://dev.mysql.com/doc/refman/8.4/en/column-indexes.html#column-indexes-prefix
            primary key(db_name(63))
        );
    "#,
    )
    .await?;

    let db_name = MySql::db_name(args);
    do_cleanup(&mut conn, &db_name).await?;

    query("insert into _sqlx_test_databases(db_name, test_path) values (?, ?)")
        .bind(&db_name)
        .bind(args.test_path)
        .execute(&mut *conn)
        .await?;

    // Try to use template cloning if migrations are provided
    let from_template = if let Some(migrator) = args.migrator {
        match get_or_create_template(&mut conn, &master_opts, migrator).await {
            Ok(Some(template_name)) => {
                // Clone from template (fast path)
                match clone_database(&mut conn, &master_opts, &template_name, &db_name).await {
                    Ok(()) => {
                        eprintln!("cloned database {db_name} from template {template_name}");
                        true
                    }
                    Err(e) => {
                        // Clean up partial database and fall back to empty database
                        eprintln!(
                            "failed to clone template, falling back to empty database: {}",
                            e
                        );
                        conn.execute(AssertSqlSafe(format!(
                            "drop database if exists `{db_name}`"
                        )))
                        .await
                        .ok();
                        conn.execute(AssertSqlSafe(format!("create database `{db_name}`")))
                            .await?;
                        eprintln!("created database {db_name}");
                        false
                    }
                }
            }
            Ok(None) => {
                // Templates disabled or not available
                conn.execute(AssertSqlSafe(format!("create database `{db_name}`")))
                    .await?;
                eprintln!("created database {db_name}");
                false
            }
            Err(e) => {
                // Template creation failed, fall back to empty database
                eprintln!(
                    "failed to create template, falling back to empty database: {}",
                    e
                );
                conn.execute(AssertSqlSafe(format!("create database `{db_name}`")))
                    .await?;
                eprintln!("created database {db_name}");
                false
            }
        }
    } else {
        // No migrations, create empty database
        conn.execute(AssertSqlSafe(format!("create database `{db_name}`")))
            .await?;
        eprintln!("created database {db_name}");
        false
    };

    Ok(TestContext {
        pool_opts: PoolOptions::new()
            // Don't allow a single test to take all the connections.
            // Most tests shouldn't require more than 5 connections concurrently,
            // or else they're likely doing too much in one test.
            .max_connections(5)
            // Close connections ASAP if left in the idle queue.
            .idle_timeout(Some(Duration::from_secs(1)))
            .parent(master_pool.clone()),
        connect_opts: master_pool
            .connect_options()
            .deref()
            .clone()
            .database(&db_name),
        db_name,
        from_template,
    })
}

async fn do_cleanup(conn: &mut MySqlConnection, db_name: &str) -> Result<(), Error> {
    let delete_db_command = format!("drop database if exists {db_name};");
    conn.execute(AssertSqlSafe(delete_db_command)).await?;
    query("delete from _sqlx_test_databases where db_name = ?")
        .bind(db_name)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

async fn cleanup_old_dbs(conn: &mut MySqlConnection) -> Result<(), Error> {
    let res: Result<Vec<u64>, Error> = query_scalar("select db_id from _sqlx_test_databases")
        .fetch_all(&mut *conn)
        .await;

    let db_ids = match res {
        Ok(db_ids) => db_ids,
        Err(e) => {
            if let Some(dbe) = e.as_database_error() {
                match dbe.downcast_ref::<MySqlDatabaseError>().number() {
                    // Column `db_id` does not exist:
                    // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_bad_field_error
                    //
                    // The table has already been migrated.
                    1054 => return Ok(()),
                    // Table `_sqlx_test_databases` does not exist.
                    // No cleanup needed.
                    // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_no_such_table
                    1146 => return Ok(()),
                    _ => (),
                }
            }

            return Err(e);
        }
    };

    // Drop old-style test databases.
    for id in db_ids {
        match conn
            .execute(AssertSqlSafe(format!(
                "drop database if exists _sqlx_test_database_{id}"
            )))
            .await
        {
            Ok(_deleted) => (),
            // Assume a database error just means the DB is still in use.
            Err(Error::Database(dbe)) => {
                eprintln!("could not clean old test database _sqlx_test_database_{id}: {dbe}");
            }
            // Bubble up other errors
            Err(e) => return Err(e),
        }
    }

    conn.execute("drop table if exists _sqlx_test_databases")
        .await?;

    Ok(())
}

fn once_lock_try_insert_polyfill<T>(this: &OnceLock<T>, value: T) -> Result<&T, (&T, T)> {
    let mut value = Some(value);
    let res = this.get_or_init(|| value.take().unwrap());
    match value {
        None => Ok(res),
        Some(value) => Err((res, value)),
    }
}

/// Helper function to replace database references in CREATE statements.
/// Used when cloning objects from template to target database.
fn replace_database_refs(sql: &str, template_name: &str, new_db_name: &str) -> String {
    // Replace backtick-quoted references first (more specific)
    let result = sql.replace(
        &format!("`{template_name}`."),
        &format!("`{new_db_name}`."),
    );
    // Then replace unquoted references
    result.replace(
        &format!("{template_name}."),
        &format!("{new_db_name}."),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_database_refs_with_backticks() {
        let sql = "CREATE VIEW `template_db`.`my_view` AS SELECT * FROM `template_db`.`users`";
        let result = replace_database_refs(sql, "template_db", "new_db");

        assert!(result.contains("`new_db`.`my_view`"));
        assert!(result.contains("`new_db`.`users`"));
        assert!(!result.contains("template_db"));
    }

    #[test]
    fn replace_database_refs_without_backticks() {
        let sql = "CREATE VIEW template_db.my_view AS SELECT * FROM template_db.users";
        let result = replace_database_refs(sql, "template_db", "new_db");

        assert!(result.contains("new_db.my_view"));
        assert!(result.contains("new_db.users"));
        assert!(!result.contains("template_db"));
    }

    #[test]
    fn replace_database_refs_mixed_quotes() {
        let sql = "CREATE VIEW `template_db`.my_view AS SELECT * FROM template_db.`users`";
        let result = replace_database_refs(sql, "template_db", "new_db");

        assert!(result.contains("`new_db`.my_view"));
        assert!(result.contains("new_db.`users`"));
        assert!(!result.contains("template_db"));
    }

    #[test]
    fn replace_database_refs_preserves_column_names() {
        // Ensure we don't accidentally replace column names that happen to match
        let sql = "CREATE VIEW `db`.`v` AS SELECT template_db_id FROM `db`.`t`";
        let result = replace_database_refs(sql, "db", "new_db");

        // Should still have the column name unchanged (no dot after it)
        assert!(result.contains("template_db_id"));
        assert!(result.contains("`new_db`.`v`"));
        assert!(result.contains("`new_db`.`t`"));
    }

    #[test]
    fn replace_database_refs_handles_definer() {
        // DEFINER clauses include user@host, not database references
        let sql = "CREATE DEFINER=`root`@`localhost` VIEW `template`.`v` AS SELECT 1";
        let result = replace_database_refs(sql, "template", "new_db");

        // DEFINER should be unchanged (no template. pattern)
        assert!(result.contains("DEFINER=`root`@`localhost`"));
        assert!(result.contains("`new_db`.`v`"));
    }

    #[test]
    fn templates_enabled_returns_false_by_default() {
        // Clear the env var if set
        std::env::remove_var(SQLX_TEST_TEMPLATE);

        assert!(!templates_enabled(), "templates should be disabled by default");
    }

    #[test]
    fn templates_enabled_returns_true_when_env_set() {
        // Set the env var
        std::env::set_var(SQLX_TEST_TEMPLATE, "1");

        assert!(templates_enabled(), "templates should be enabled when env var is set");

        // Clean up
        std::env::remove_var(SQLX_TEST_TEMPLATE);
    }

    #[test]
    fn templates_enabled_accepts_any_value() {
        // Any value should enable templates
        std::env::set_var(SQLX_TEST_TEMPLATE, "true");
        assert!(templates_enabled());

        std::env::set_var(SQLX_TEST_TEMPLATE, "yes");
        assert!(templates_enabled());

        std::env::set_var(SQLX_TEST_TEMPLATE, "");
        assert!(templates_enabled(), "even empty string should enable templates");

        // Clean up
        std::env::remove_var(SQLX_TEST_TEMPLATE);
    }

    #[test]
    fn format_column_list_single_column() {
        assert_eq!(format_column_list("id"), "`id`");
    }

    #[test]
    fn format_column_list_multiple_columns() {
        assert_eq!(format_column_list("id,name,value"), "`id`, `name`, `value`");
    }

    #[test]
    fn format_column_list_trims_whitespace() {
        assert_eq!(
            format_column_list("id , name , value"),
            "`id`, `name`, `value`"
        );
    }

    #[test]
    fn format_fk_action_cascade() {
        assert_eq!(format_fk_action("CASCADE"), "ON DELETE CASCADE");
    }

    #[test]
    fn format_fk_action_restrict() {
        assert_eq!(format_fk_action("RESTRICT"), "ON DELETE RESTRICT");
    }

    #[test]
    fn format_fk_action_set_null() {
        assert_eq!(format_fk_action("SET NULL"), "ON DELETE SET NULL");
    }

    #[test]
    fn format_fk_action_no_action_is_empty() {
        // NO ACTION is the default, so we don't need to specify it
        assert_eq!(format_fk_action("NO ACTION"), "");
    }

    #[test]
    fn format_fk_update_action_cascade() {
        assert_eq!(format_fk_update_action("CASCADE"), "ON UPDATE CASCADE");
    }

    #[test]
    fn format_fk_update_action_set_null() {
        assert_eq!(format_fk_update_action("SET NULL"), "ON UPDATE SET NULL");
    }

    #[test]
    fn format_fk_update_action_no_action_is_empty() {
        assert_eq!(format_fk_update_action("NO ACTION"), "");
    }
}
