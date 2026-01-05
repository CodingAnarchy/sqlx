use std::future::Future;
use std::time::Duration;

use base64::{
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
    Engine as _,
};
pub use fixtures::FixtureSnapshot;
use sha2::{Digest, Sha256, Sha512};

use crate::connection::{ConnectOptions, Connection};
use crate::database::Database;
use crate::error::Error;
use crate::executor::Executor;
use crate::migrate::{Migrate, Migrator};
use crate::pool::{Pool, PoolConnection, PoolOptions};

mod fixtures;

/// Compute a combined hash of all migrations for template invalidation.
///
/// This hash is used to name template databases. When migrations change,
/// a new hash is generated, resulting in a new template being created.
pub fn migrations_hash(migrator: &Migrator) -> String {
    let mut hasher = Sha256::new();

    for migration in migrator.iter() {
        // Include version, type, and checksum in the hash
        hasher.update(migration.version.to_le_bytes());
        hasher.update([migration.migration_type as u8]);
        hasher.update(&*migration.checksum);
    }

    let hash = hasher.finalize();
    // Use first 16 bytes (128 bits) for a reasonably short but unique name
    URL_SAFE_NO_PAD.encode(&hash[..16])
}

/// Generate a template database name from a migrations hash.
///
/// Template names follow the pattern `_sqlx_template_<hash>` and are kept
/// under 63 characters to respect database identifier limits.
pub fn template_db_name(migrations_hash: &str) -> String {
    // Replace any characters that might cause issues in database names
    let safe_hash = migrations_hash.replace(['-', '+', '/'], "_");
    format!("_sqlx_template_{}", safe_hash)
}

pub trait TestSupport: Database {
    /// Get parameters to construct a `Pool` suitable for testing.
    ///
    /// This `Pool` instance will behave somewhat specially:
    /// * all handles share a single global semaphore to avoid exceeding the connection limit
    ///   on the database server.
    /// * each invocation results in a different temporary database.
    ///
    /// The implementation may require `DATABASE_URL` to be set in order to manage databases.
    /// The user credentials it contains must have the privilege to create and drop databases.
    fn test_context(
        args: &TestArgs,
    ) -> impl Future<Output = Result<TestContext<Self>, Error>> + Send + '_;

    fn cleanup_test(db_name: &str) -> impl Future<Output = Result<(), Error>> + Send + '_;

    /// Cleanup any test databases that are no longer in-use.
    ///
    /// Returns a count of the databases deleted, if possible.
    ///
    /// The implementation may require `DATABASE_URL` to be set in order to manage databases.
    /// The user credentials it contains must have the privilege to create and drop databases.
    fn cleanup_test_dbs() -> impl Future<Output = Result<Option<usize>, Error>> + Send + 'static;

    /// Take a snapshot of the current state of the database (data only).
    ///
    /// This snapshot can then be used to generate test fixtures.
    fn snapshot(
        conn: &mut Self::Connection,
    ) -> impl Future<Output = Result<FixtureSnapshot<Self>, Error>> + Send + '_;

    /// Generate a unique database name for the given test path.
    fn db_name(args: &TestArgs) -> String {
        let mut hasher = Sha512::new();
        hasher.update(args.test_path.as_bytes());
        let hash = hasher.finalize();
        let hash = URL_SAFE.encode(&hash[..39]);
        let db_name = format!("_sqlx_test_{}", hash).replace('-', "_");
        debug_assert!(db_name.len() == 63);
        db_name
    }
}

pub struct TestFixture {
    pub path: &'static str,
    pub contents: &'static str,
}

pub struct TestArgs {
    pub test_path: &'static str,
    pub migrator: Option<&'static Migrator>,
    pub fixtures: &'static [TestFixture],
}

pub trait TestFn {
    type Output;

    fn run_test(self, args: TestArgs) -> Self::Output;
}

pub trait TestTermination {
    fn is_success(&self) -> bool;
}

pub struct TestContext<DB: Database> {
    pub pool_opts: PoolOptions<DB>,
    pub connect_opts: <DB::Connection as Connection>::Options,
    pub db_name: String,
    /// Whether this test database was created from a template.
    /// When true, migrations have already been applied and should be skipped.
    pub from_template: bool,
}

impl<DB, Fut> TestFn for fn(Pool<DB>) -> Fut
where
    DB: TestSupport + Database,
    DB::Connection: Migrate,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    Fut: Future,
    Fut::Output: TestTermination,
{
    type Output = Fut::Output;

    fn run_test(self, args: TestArgs) -> Self::Output {
        run_test_with_pool(args, self)
    }
}

impl<DB, Fut> TestFn for fn(PoolConnection<DB>) -> Fut
where
    DB: TestSupport + Database,
    DB::Connection: Migrate,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    Fut: Future,
    Fut::Output: TestTermination,
{
    type Output = Fut::Output;

    fn run_test(self, args: TestArgs) -> Self::Output {
        run_test_with_pool(args, |pool| async move {
            let conn = pool
                .acquire()
                .await
                .expect("failed to acquire test pool connection");
            let res = (self)(conn).await;
            pool.close().await;
            res
        })
    }
}

impl<DB, Fut> TestFn for fn(PoolOptions<DB>, <DB::Connection as Connection>::Options) -> Fut
where
    DB: Database + TestSupport,
    DB::Connection: Migrate,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    Fut: Future,
    Fut::Output: TestTermination,
{
    type Output = Fut::Output;

    fn run_test(self, args: TestArgs) -> Self::Output {
        run_test(args, self)
    }
}

impl<Fut> TestFn for fn() -> Fut
where
    Fut: Future,
{
    type Output = Fut::Output;

    fn run_test(self, args: TestArgs) -> Self::Output {
        assert!(
            args.fixtures.is_empty(),
            "fixtures cannot be applied for a bare function"
        );
        crate::rt::test_block_on(self())
    }
}

impl TestArgs {
    pub fn new(test_path: &'static str) -> Self {
        TestArgs {
            test_path,
            migrator: None,
            fixtures: &[],
        }
    }

    pub fn migrator(&mut self, migrator: &'static Migrator) {
        self.migrator = Some(migrator);
    }

    pub fn fixtures(&mut self, fixtures: &'static [TestFixture]) {
        self.fixtures = fixtures;
    }
}

impl TestTermination for () {
    fn is_success(&self) -> bool {
        true
    }
}

impl<T, E> TestTermination for Result<T, E> {
    fn is_success(&self) -> bool {
        self.is_ok()
    }
}

fn run_test_with_pool<DB, F, Fut>(args: TestArgs, test_fn: F) -> Fut::Output
where
    DB: TestSupport,
    DB::Connection: Migrate,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    F: FnOnce(Pool<DB>) -> Fut,
    Fut: Future,
    Fut::Output: TestTermination,
{
    let test_path = args.test_path;
    run_test::<DB, _, _>(args, |pool_opts, connect_opts| async move {
        let pool = pool_opts
            .connect_with(connect_opts)
            .await
            .expect("failed to connect test pool");

        let res = test_fn(pool.clone()).await;

        let close_timed_out = crate::rt::timeout(Duration::from_secs(10), pool.close())
            .await
            .is_err();

        if close_timed_out {
            eprintln!("test {test_path} held onto Pool after exiting");
        }

        res
    })
}

fn run_test<DB, F, Fut>(args: TestArgs, test_fn: F) -> Fut::Output
where
    DB: TestSupport,
    DB::Connection: Migrate,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    F: FnOnce(PoolOptions<DB>, <DB::Connection as Connection>::Options) -> Fut,
    Fut: Future,
    Fut::Output: TestTermination,
{
    crate::rt::test_block_on(async move {
        let test_context = DB::test_context(&args)
            .await
            .expect("failed to connect to setup test database");

        setup_test_db::<DB>(
            &test_context.connect_opts,
            &args,
            test_context.from_template,
        )
        .await;

        let res = test_fn(test_context.pool_opts, test_context.connect_opts).await;

        if res.is_success() {
            if let Err(e) = DB::cleanup_test(&DB::db_name(&args)).await {
                eprintln!(
                    "failed to delete database {:?}: {}",
                    test_context.db_name, e
                );
            }
        }

        res
    })
}

async fn setup_test_db<DB: Database>(
    copts: &<DB::Connection as Connection>::Options,
    args: &TestArgs,
    from_template: bool,
) where
    DB::Connection: Migrate + Sized,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    let mut conn = copts
        .connect()
        .await
        .expect("failed to connect to test database");

    // Skip migrations if the database was cloned from a template
    // (migrations were already applied to the template)
    if !from_template {
        if let Some(migrator) = args.migrator {
            migrator
                .run_direct(None, &mut conn)
                .await
                .expect("failed to apply migrations");
        }
    }

    for fixture in args.fixtures {
        (&mut conn)
            .execute(fixture.contents)
            .await
            .unwrap_or_else(|e| panic!("failed to apply test fixture {:?}: {:?}", fixture.path, e));
    }

    conn.close()
        .await
        .expect("failed to close setup connection");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::{Migration, MigrationType, Migrator};
    use crate::sql_str::SqlSafeStr;
    use std::borrow::Cow;

    fn create_test_migrator(migrations: Vec<Migration>) -> Migrator {
        Migrator {
            migrations: Cow::Owned(migrations),
            ignore_missing: false,
            locking: true,
            no_tx: false,
            table_name: Cow::Borrowed("_sqlx_migrations"),
            create_schemas: Cow::Borrowed(&[]),
        }
    }

    fn create_test_migration(version: i64, sql: &'static str) -> Migration {
        Migration::new(
            version,
            Cow::Borrowed("test migration"),
            MigrationType::Simple,
            sql.into_sql_str(),
            false,
        )
    }

    #[test]
    fn migrations_hash_is_deterministic() {
        let migrations = vec![
            create_test_migration(1, "CREATE TABLE users (id INT)"),
            create_test_migration(2, "CREATE TABLE posts (id INT)"),
            create_test_migration(3, "ALTER TABLE users ADD name VARCHAR(255)"),
        ];
        let migrator = create_test_migrator(migrations);

        // Hash should be consistent across calls
        let hash1 = migrations_hash(&migrator);
        let hash2 = migrations_hash(&migrator);

        assert_eq!(hash1, hash2, "migrations_hash should be deterministic");

        // Hash should be non-empty and reasonable length
        assert!(!hash1.is_empty(), "hash should not be empty");
        assert!(
            hash1.len() < 30,
            "hash should be reasonably short for use in database names"
        );
    }

    #[test]
    fn migrations_hash_changes_with_different_migrations() {
        let migrations1 = vec![create_test_migration(1, "CREATE TABLE users (id INT)")];
        let migrations2 = vec![create_test_migration(1, "CREATE TABLE posts (id INT)")];

        let hash1 = migrations_hash(&create_test_migrator(migrations1));
        let hash2 = migrations_hash(&create_test_migrator(migrations2));

        assert_ne!(hash1, hash2, "different migrations should produce different hashes");
    }

    #[test]
    fn migrations_hash_changes_with_version() {
        let migrations1 = vec![create_test_migration(1, "CREATE TABLE users (id INT)")];
        let migrations2 = vec![create_test_migration(2, "CREATE TABLE users (id INT)")];

        let hash1 = migrations_hash(&create_test_migrator(migrations1));
        let hash2 = migrations_hash(&create_test_migrator(migrations2));

        assert_ne!(hash1, hash2, "different versions should produce different hashes");
    }

    #[test]
    fn template_db_name_has_correct_format() {
        let name = template_db_name("abc123xyz");

        assert!(
            name.starts_with("_sqlx_template_"),
            "template name should have correct prefix"
        );
        assert!(
            name.contains("abc123xyz"),
            "template name should contain hash"
        );
        assert!(
            name.len() < 63,
            "template name should fit in MySQL identifier limit"
        );
    }

    #[test]
    fn template_db_name_escapes_special_characters() {
        // Test with special characters that need escaping
        let name_with_special = template_db_name("a-b+c/d");

        assert!(
            !name_with_special.contains('-'),
            "should not contain hyphen"
        );
        assert!(!name_with_special.contains('+'), "should not contain plus");
        assert!(!name_with_special.contains('/'), "should not contain slash");
        assert!(
            name_with_special.starts_with("_sqlx_template_"),
            "should still have correct prefix"
        );
    }

    #[test]
    fn template_db_name_handles_empty_hash() {
        let name = template_db_name("");

        assert!(
            name.starts_with("_sqlx_template_"),
            "should have prefix even with empty hash"
        );
        assert_eq!(name, "_sqlx_template_");
    }
}
