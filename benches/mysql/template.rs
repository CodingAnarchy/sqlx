//! Benchmarks for MySQL template database cloning.
//!
//! This benchmark measures the performance improvement from using template
//! databases vs applying migrations fresh for each test.
//!
//! Run with: cargo bench --bench mysql-template --features mysql,runtime-tokio
//!
//! Requires DATABASE_URL environment variable to be set.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use sqlx::mysql::MySqlConnection;
use sqlx::{Connection, Executor};
use sqlx_core::sql_str::AssertSqlSafe;
use std::time::Duration;

/// Column types for realistic schema variety
const COLUMN_TYPES: &[&str] = &[
    "VARCHAR(255)",
    "TEXT",
    "INT",
    "BIGINT",
    "DECIMAL(10,2)",
    "BOOLEAN",
    "DATE",
    "DATETIME",
    "JSON",
    "MEDIUMBLOB",
];

/// Generate synthetic migrations for benchmarking.
/// Creates 18 tables with varied column types, indexes, FULLTEXT indexes, and triggers.
/// Designed to simulate real-world schema complexity.
fn generate_migrations() -> Vec<String> {
    let mut migrations = Vec::with_capacity(150);

    // Create 18 tables with initial migrations
    for i in 0..18 {
        migrations.push(format!(
            "CREATE TABLE bench_table_{i} (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                uuid CHAR(36) NOT NULL,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                amount DECIMAL(10,2) DEFAULT 0.00,
                status ENUM('active', 'inactive', 'pending') DEFAULT 'pending',
                metadata JSON,
                is_deleted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_{i}_name (name),
                UNIQUE INDEX idx_{i}_uuid (uuid)
            )"
        ));
    }

    // Add ~40 ALTER TABLE migrations (add columns with varied types)
    for i in 18..58 {
        let table_num = i % 18;
        let col_type = COLUMN_TYPES[i % COLUMN_TYPES.len()];
        migrations.push(format!(
            "ALTER TABLE bench_table_{table_num} ADD COLUMN field_{i} {col_type}"
        ));
    }

    // Add ~20 index migrations on existing columns
    // Each table (0-17) has columns: field_X where X % 18 == table_num
    // So table 0 has field_18, field_36, field_54; table 1 has field_19, field_37, field_55; etc.
    for table_num in 0..18 {
        // Find a column that exists on this table and supports indexing
        for j in 0..3 {
            let field_num = 18 + table_num + (j * 18);
            if field_num >= 58 {
                break; // Fields only go up to 57
            }
            let col_idx = field_num % COLUMN_TYPES.len();
            // Only add index if the column type supports it (not TEXT/BLOB/JSON)
            if col_idx < 5 && col_idx != 1 {
                // VARCHAR, INT, BIGINT, DECIMAL (skip TEXT)
                migrations.push(format!(
                    "CREATE INDEX idx_{table_num}_field_{field_num} ON bench_table_{table_num} (field_{field_num})"
                ));
                break; // One index per table
            }
        }
    }

    // Add ~12 foreign key migrations (linking tables together)
    for i in 78..90 {
        let table_num = (i % 17) + 1; // Tables 1-17
        migrations.push(format!(
            "ALTER TABLE bench_table_{table_num} ADD COLUMN ref_table_0_id_{i} BIGINT, \
             ADD CONSTRAINT fk_{i}_to_0 FOREIGN KEY (ref_table_0_id_{i}) REFERENCES bench_table_0(id)"
        ));
    }

    // Add ~10 more column migrations
    for i in 90..100 {
        let table_num = i % 18;
        let col_type = COLUMN_TYPES[i % COLUMN_TYPES.len()];
        let nullable = if i % 2 == 0 { "NULL" } else { "" };
        migrations.push(format!(
            "ALTER TABLE bench_table_{table_num} ADD COLUMN extra_{i} {col_type} {nullable}"
        ));
    }

    // Add FULLTEXT indexes (common in real apps for search functionality)
    // These are more expensive to create than regular indexes
    for i in 0..6 {
        migrations.push(format!(
            "ALTER TABLE bench_table_{i} ADD FULLTEXT INDEX ft_idx_{i} (name, description)"
        ));
    }

    // Add audit/history tables with triggers (common pattern for data tracking)
    for i in 0..3 {
        // Create history table
        migrations.push(format!(
            "CREATE TABLE bench_table_{i}_history (
                history_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                original_id BIGINT NOT NULL,
                name VARCHAR(255),
                description TEXT,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_history_{i}_original (original_id)
            )"
        ));

        // Create trigger to track changes
        migrations.push(format!(
            "CREATE TRIGGER trg_bench_table_{i}_audit \
             AFTER UPDATE ON bench_table_{i} \
             FOR EACH ROW \
             INSERT INTO bench_table_{i}_history (original_id, name, description) \
             VALUES (OLD.id, OLD.name, OLD.description)"
        ));
    }

    // Add more ALTER TABLE migrations to reach 150
    for i in 112..150 {
        let table_num = i % 18;
        migrations.push(format!(
            "ALTER TABLE bench_table_{table_num} ADD COLUMN extra_field_{i} VARCHAR(100)"
        ));
    }

    migrations
}

/// Apply all migrations to a database
async fn apply_migrations(conn: &mut MySqlConnection, migrations: &[String]) {
    for migration in migrations {
        conn.execute(AssertSqlSafe(migration.clone())).await.unwrap();
    }
}

/// Create a unique database for benchmarking
async fn create_bench_db(master_conn: &mut MySqlConnection, db_name: &str) {
    master_conn
        .execute(AssertSqlSafe(format!(
            "CREATE DATABASE IF NOT EXISTS `{db_name}`"
        )))
        .await
        .unwrap();
}

/// Drop a benchmark database
async fn drop_bench_db(master_conn: &mut MySqlConnection, db_name: &str) {
    let _ = master_conn
        .execute(AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS `{db_name}`"
        )))
        .await;
}

/// Clone a database by copying all tables
async fn clone_database(database_url: &str, source_db: &str, target_db: &str) {
    let mut master_conn = MySqlConnection::connect(database_url).await.unwrap();

    // Create target database
    master_conn
        .execute(AssertSqlSafe(format!("CREATE DATABASE `{target_db}`")))
        .await
        .unwrap();

    // Get all tables from source
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
    )
    .bind(source_db)
    .fetch_all(&mut master_conn)
    .await
    .unwrap();

    // Copy each table
    for (table,) in tables {
        // Create table structure
        master_conn
            .execute(AssertSqlSafe(format!(
                "CREATE TABLE `{target_db}`.`{table}` LIKE `{source_db}`.`{table}`"
            )))
            .await
            .unwrap();

        // Copy data (in real usage, migrations table data is copied)
        master_conn
            .execute(AssertSqlSafe(format!(
                "INSERT INTO `{target_db}`.`{table}` SELECT * FROM `{source_db}`.`{table}`"
            )))
            .await
            .unwrap();
    }

    master_conn.close().await.unwrap();
}

fn bench_fresh_migrations(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let migrations = generate_migrations();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let bench_db = "_sqlx_bench_fresh";

    // Ensure clean state
    runtime.block_on(async {
        let mut conn = MySqlConnection::connect(&database_url).await.unwrap();
        drop_bench_db(&mut conn, bench_db).await;
        conn.close().await.unwrap();
    });

    c.bench_with_input(
        BenchmarkId::new("fresh_migrations", "150_migrations"),
        &migrations,
        |b, migrations| {
            let database_url = database_url.clone();
            let migrations = migrations.clone();

            b.to_async(&runtime).iter(|| {
                let database_url = database_url.clone();
                let migrations = migrations.clone();

                async move {
                    let mut master_conn =
                        MySqlConnection::connect(&database_url).await.unwrap();

                    // Create fresh database
                    create_bench_db(&mut master_conn, bench_db).await;

                    // Connect to benchmark database
                    let bench_url = format!(
                        "{}/{}",
                        database_url.rsplit_once('/').unwrap().0,
                        bench_db
                    );
                    let mut conn = MySqlConnection::connect(&bench_url).await.unwrap();

                    // Apply all migrations
                    apply_migrations(&mut conn, &migrations).await;

                    conn.close().await.unwrap();

                    // Cleanup
                    drop_bench_db(&mut master_conn, bench_db).await;
                    master_conn.close().await.unwrap();
                }
            });
        },
    );
}

fn bench_template_clone(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let migrations = generate_migrations();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let template_db = "_sqlx_bench_template";
    let clone_db = "_sqlx_bench_clone";

    // Create template
    runtime.block_on(async {
        let mut master_conn = MySqlConnection::connect(&database_url).await.unwrap();

        drop_bench_db(&mut master_conn, template_db).await;
        drop_bench_db(&mut master_conn, clone_db).await;
        create_bench_db(&mut master_conn, template_db).await;

        let template_url = format!(
            "{}/{}",
            database_url.rsplit_once('/').unwrap().0,
            template_db
        );
        let mut template_conn = MySqlConnection::connect(&template_url).await.unwrap();
        apply_migrations(&mut template_conn, &migrations).await;
        template_conn.close().await.unwrap();
        master_conn.close().await.unwrap();
    });

    c.bench_function("template_clone/150_migrations", |b| {
        let database_url = database_url.clone();

        b.to_async(&runtime).iter(|| {
            let database_url = database_url.clone();

            async move {
                // Clone from template
                clone_database(&database_url, template_db, clone_db).await;

                // Cleanup cloned database
                let mut master_conn =
                    MySqlConnection::connect(&database_url).await.unwrap();
                drop_bench_db(&mut master_conn, clone_db).await;
                master_conn.close().await.unwrap();
            }
        });
    });

    // Cleanup template
    runtime.block_on(async {
        let mut master_conn = MySqlConnection::connect(&database_url).await.unwrap();
        drop_bench_db(&mut master_conn, template_db).await;
        master_conn.close().await.unwrap();
    });
}

/// Benchmark parallel test setup with fresh migrations
fn bench_parallel_fresh(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let migrations = generate_migrations();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let concurrency = 8;

    c.bench_function(
        &format!("parallel_fresh/{}_concurrent", concurrency),
        |b| {
            let database_url = database_url.clone();
            let migrations = migrations.clone();

            b.to_async(&runtime).iter(|| {
                let database_url = database_url.clone();
                let migrations = migrations.clone();

                async move {
                    let mut handles = Vec::new();

                    for i in 0..concurrency {
                        let database_url = database_url.clone();
                        let migrations = migrations.clone();
                        let bench_db = format!("_sqlx_bench_parallel_{}", i);

                        let handle = tokio::spawn(async move {
                            let mut master_conn =
                                MySqlConnection::connect(&database_url).await.unwrap();

                            // Drop and recreate database
                            drop_bench_db(&mut master_conn, &bench_db).await;
                            create_bench_db(&mut master_conn, &bench_db).await;

                            // Connect to benchmark database
                            let bench_url = format!(
                                "{}/{}",
                                database_url.rsplit_once('/').unwrap().0,
                                bench_db
                            );
                            let mut conn = MySqlConnection::connect(&bench_url).await.unwrap();

                            // Apply all migrations
                            apply_migrations(&mut conn, &migrations).await;

                            conn.close().await.unwrap();

                            // Cleanup
                            drop_bench_db(&mut master_conn, &bench_db).await;
                            master_conn.close().await.unwrap();
                        });

                        handles.push(handle);
                    }

                    // Wait for all to complete
                    for handle in handles {
                        handle.await.unwrap();
                    }
                }
            });
        },
    );
}

/// Benchmark parallel test setup with template cloning
fn bench_parallel_clone(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let migrations = generate_migrations();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let template_db = "_sqlx_bench_parallel_template";
    let concurrency = 8;

    // Create template first
    runtime.block_on(async {
        let mut master_conn = MySqlConnection::connect(&database_url).await.unwrap();

        drop_bench_db(&mut master_conn, template_db).await;
        create_bench_db(&mut master_conn, template_db).await;

        let template_url = format!(
            "{}/{}",
            database_url.rsplit_once('/').unwrap().0,
            template_db
        );
        let mut template_conn = MySqlConnection::connect(&template_url).await.unwrap();
        apply_migrations(&mut template_conn, &migrations).await;
        template_conn.close().await.unwrap();
        master_conn.close().await.unwrap();
    });

    c.bench_function(
        &format!("parallel_clone/{}_concurrent", concurrency),
        |b| {
            let database_url = database_url.clone();

            b.to_async(&runtime).iter(|| {
                let database_url = database_url.clone();

                async move {
                    let mut handles = Vec::new();

                    for i in 0..concurrency {
                        let database_url = database_url.clone();
                        let clone_db = format!("_sqlx_bench_clone_{}", i);

                        let handle = tokio::spawn(async move {
                            // Clone from template
                            clone_database(&database_url, template_db, &clone_db).await;

                            // Cleanup
                            let mut master_conn =
                                MySqlConnection::connect(&database_url).await.unwrap();
                            drop_bench_db(&mut master_conn, &clone_db).await;
                            master_conn.close().await.unwrap();
                        });

                        handles.push(handle);
                    }

                    // Wait for all to complete
                    for handle in handles {
                        handle.await.unwrap();
                    }
                }
            });
        },
    );

    // Cleanup template
    runtime.block_on(async {
        let mut master_conn = MySqlConnection::connect(&database_url).await.unwrap();
        drop_bench_db(&mut master_conn, template_db).await;
        master_conn.close().await.unwrap();
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(30));
    targets = bench_fresh_migrations, bench_template_clone, bench_parallel_fresh, bench_parallel_clone
}
criterion_main!(benches);
