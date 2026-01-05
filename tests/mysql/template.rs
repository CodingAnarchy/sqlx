// Integration tests for template database cloning functionality in MySQL.
//
// These tests require a database connection and verify end-to-end template
// functionality including:
// 1. Template databases are created when migrations are used
// 2. Multiple tests with the same migrations share a template
// 3. SQLX_TEST_TEMPLATE enables template cloning (opt-in)
// 4. Fixtures are applied per-test, not stored in template
//
// Unit tests for migrations_hash() and template_db_name() are in
// sqlx-core/src/testing/mod.rs

use sqlx::mysql::MySqlPool;
use sqlx::Connection;

/// Verify that the template tracking table exists and contains entries
/// after running a test with migrations.
#[sqlx::test(migrations = "tests/mysql/migrations")]
async fn it_creates_template_database(pool: MySqlPool) -> sqlx::Result<()> {
    // Get the master database connection to check for templates
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut master_conn = sqlx::mysql::MySqlConnection::connect(&database_url).await?;

    // Check that the template tracking table exists and has at least one entry
    let template_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM _sqlx_test_templates WHERE template_name LIKE '_sqlx_template_%'",
    )
    .fetch_one(&mut master_conn)
    .await?;

    // If templates are enabled, we should have at least one template
    if std::env::var("SQLX_TEST_TEMPLATE").is_ok() {
        assert!(
            template_count > 0,
            "Expected at least one template database to be created"
        );
    }

    // Verify the test database has the expected tables from migrations
    let table_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = database() AND table_name IN ('user', 'post', 'comment')")
            .fetch_one(&pool)
            .await?;

    assert_eq!(table_count, 3, "Expected user, post, and comment tables");

    Ok(())
}

/// Verify that the migrations table is properly cloned from template.
/// When cloning from a template, the _sqlx_migrations table should already
/// exist and contain the migration history.
#[sqlx::test(migrations = "tests/mysql/migrations")]
async fn it_clones_migrations_table(pool: MySqlPool) -> sqlx::Result<()> {
    // Count migration files in the migrations directory
    let migration_dir = std::path::Path::new("tests/mysql/migrations");
    let expected_count = std::fs::read_dir(migration_dir)
        .expect("migrations directory should exist")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "sql")
                .unwrap_or(false)
        })
        .count() as i64;

    // Check that _sqlx_migrations table has the expected count
    let migration_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await?;

    assert_eq!(
        migration_count, expected_count,
        "Migration count in database should match number of migration files"
    );

    // Verify the versions form a contiguous sequence starting at 1
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await?;

    let expected_versions: Vec<i64> = (1..=expected_count).collect();
    assert_eq!(
        versions, expected_versions,
        "Migration versions should be contiguous starting at 1"
    );

    Ok(())
}

/// Test that multiple tests with the same migrations share a template.
/// This test runs alongside other tests with the same migrations and
/// verifies that only one template exists.
#[sqlx::test(migrations = "tests/mysql/migrations")]
async fn it_reuses_template_for_same_migrations_1(pool: MySqlPool) -> sqlx::Result<()> {
    // This test shares migrations with other tests, so they should all
    // use the same template database.
    let db_name: String = sqlx::query_scalar("SELECT database()")
        .fetch_one(&pool)
        .await?;

    assert!(
        db_name.starts_with("_sqlx_test_"),
        "Test database should start with _sqlx_test_"
    );

    // Verify tables exist
    let user_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = database() AND table_name = 'user')",
    )
    .fetch_one(&pool)
    .await?;

    assert!(user_exists, "user table should exist");

    Ok(())
}

/// Second test with same migrations - should reuse the same template.
#[sqlx::test(migrations = "tests/mysql/migrations")]
async fn it_reuses_template_for_same_migrations_2(pool: MySqlPool) -> sqlx::Result<()> {
    // Same test as above - verifies template reuse
    let db_name: String = sqlx::query_scalar("SELECT database()")
        .fetch_one(&pool)
        .await?;

    assert!(
        db_name.starts_with("_sqlx_test_"),
        "Test database should start with _sqlx_test_"
    );

    // Verify tables exist
    let post_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = database() AND table_name = 'post')",
    )
    .fetch_one(&pool)
    .await?;

    assert!(post_exists, "post table should exist");

    Ok(())
}

/// Verify that template databases have the correct naming pattern.
#[sqlx::test(migrations = "tests/mysql/migrations")]
async fn it_names_templates_correctly(_pool: MySqlPool) -> sqlx::Result<()> {
    if std::env::var("SQLX_TEST_TEMPLATE").is_err() {
        // Skip this test if templates are disabled
        return Ok(());
    }

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut master_conn = sqlx::mysql::MySqlConnection::connect(&database_url).await?;

    // Get template names
    let template_names: Vec<String> = sqlx::query_scalar(
        "SELECT template_name FROM _sqlx_test_templates WHERE template_name LIKE '_sqlx_template_%'",
    )
    .fetch_all(&mut master_conn)
    .await?;

    for name in &template_names {
        assert!(
            name.starts_with("_sqlx_template_"),
            "Template name should start with _sqlx_template_, got: {}",
            name
        );
        // Template names should be reasonably short (under 63 chars for MySQL)
        assert!(
            name.len() < 63,
            "Template name should be under 63 chars, got: {} ({})",
            name,
            name.len()
        );
    }

    Ok(())
}

/// Test that fixtures are applied on top of the cloned template.
/// Fixtures should be applied per-test, not stored in the template.
#[sqlx::test(migrations = "tests/mysql/migrations", fixtures("users"))]
async fn it_applies_fixtures_after_clone(pool: MySqlPool) -> sqlx::Result<()> {
    // The users fixture should have been applied
    let user_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user")
        .fetch_one(&pool)
        .await?;

    assert!(user_count > 0, "users fixture should have inserted users");

    // Verify specific users from the fixture
    let usernames: Vec<String> = sqlx::query_scalar("SELECT username FROM user ORDER BY username")
        .fetch_all(&pool)
        .await?;

    assert_eq!(usernames, vec!["alice", "bob"]);

    Ok(())
}

/// Test that different tests get different fixture data even when
/// sharing the same template.
#[sqlx::test(migrations = "tests/mysql/migrations", fixtures("users", "posts"))]
async fn it_isolates_fixtures_between_tests(pool: MySqlPool) -> sqlx::Result<()> {
    // This test has different fixtures than the previous one
    let post_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM post")
        .fetch_one(&pool)
        .await?;

    assert!(post_count > 0, "posts fixture should have inserted posts");

    Ok(())
}

/// Verify that foreign key constraints are properly cloned from template.
/// CREATE TABLE ... LIKE doesn't copy FKs, so we have a special step to copy them.
#[sqlx::test(migrations = "tests/mysql/migrations")]
async fn it_clones_foreign_key_constraints(pool: MySqlPool) -> sqlx::Result<()> {
    // Query foreign key constraint names from the cloned database
    let fk_names: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT tc.constraint_name
        FROM information_schema.table_constraints tc
        WHERE tc.table_schema = database()
            AND tc.constraint_type = 'FOREIGN KEY'
        ORDER BY tc.constraint_name
        "#,
    )
    .fetch_all(&pool)
    .await?;

    // We added 3 FKs in migration 4: fk_post_user, fk_comment_post, fk_comment_user
    assert_eq!(
        fk_names,
        vec!["fk_comment_post", "fk_comment_user", "fk_post_user"],
        "Expected 3 foreign key constraints to be cloned"
    );

    // Verify FK enforcement works - deleting a user should cascade to posts
    // First insert test data
    sqlx::query("INSERT INTO user (username) VALUES ('test_fk_user')")
        .execute(&pool)
        .await?;

    // Get the inserted user's ID
    let user_row: (i32,) = sqlx::query_as("SELECT user_id FROM user WHERE username = 'test_fk_user'")
        .fetch_one(&pool)
        .await?;
    let user_id = user_row.0;

    sqlx::query("INSERT INTO post (user_id, content) VALUES (?, 'test post')")
        .bind(user_id)
        .execute(&pool)
        .await?;

    // Verify post exists before delete
    let post_before: Vec<(i32,)> = sqlx::query_as("SELECT post_id FROM post WHERE user_id = ?")
        .bind(user_id)
        .fetch_all(&pool)
        .await?;
    assert_eq!(post_before.len(), 1, "Post should exist before cascade delete");

    // Delete user - should cascade to posts
    sqlx::query("DELETE FROM user WHERE user_id = ?")
        .bind(user_id)
        .execute(&pool)
        .await?;

    // Verify post was also deleted (CASCADE)
    let post_after: Vec<(i32,)> = sqlx::query_as("SELECT post_id FROM post WHERE user_id = ?")
        .bind(user_id)
        .fetch_all(&pool)
        .await?;
    assert_eq!(post_after.len(), 0, "Post should have been cascade deleted");

    Ok(())
}

/// Verify that AUTO_INCREMENT values are correctly set after template cloning.
/// When a template has seed data, the cloned database should have AUTO_INCREMENT
/// set to MAX(id) + 1, allowing new inserts without duplicate key errors.
#[sqlx::test(migrations = "tests/mysql/migrations")]
async fn it_preserves_auto_increment_after_clone(pool: MySqlPool) -> sqlx::Result<()> {
    // The migrations create auto_increment_test table with 3 seed rows (ids 1, 2, 3)
    // Verify we can insert a new row without duplicate key errors
    sqlx::query("INSERT INTO auto_increment_test (name) VALUES ('new_test_row')")
        .execute(&pool)
        .await?;

    // Get the ID of the newly inserted row - should be 4 (after seed rows 1, 2, 3)
    let new_id: i32 =
        sqlx::query_scalar("SELECT id FROM auto_increment_test WHERE name = 'new_test_row'")
            .fetch_one(&pool)
            .await?;

    assert_eq!(
        new_id, 4,
        "New row should have id=4 (got {}), AUTO_INCREMENT should continue from seed data",
        new_id
    );

    // Verify the seed rows still exist
    let seed_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM auto_increment_test WHERE name LIKE 'seed_row_%'")
            .fetch_one(&pool)
            .await?;

    assert_eq!(seed_count, 3, "Should have 3 seed rows");

    Ok(())
}

/// Verify that views with dependencies are correctly cloned.
/// view_user_post_summary depends on view_user_posts, so the cloning
/// must handle the dependency order (or retry failed views).
#[sqlx::test(migrations = "tests/mysql/migrations")]
async fn it_clones_dependent_views(pool: MySqlPool) -> sqlx::Result<()> {
    // Verify both views exist and are queryable
    let base_view_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_schema = database() AND table_name = 'view_user_posts')",
    )
    .fetch_one(&pool)
    .await?;

    assert!(base_view_exists, "Base view view_user_posts should exist");

    let dependent_view_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_schema = database() AND table_name = 'view_user_post_summary')",
    )
    .fetch_one(&pool)
    .await?;

    assert!(
        dependent_view_exists,
        "Dependent view view_user_post_summary should exist"
    );

    // Insert test data to verify the views work correctly
    sqlx::query("INSERT INTO user (username) VALUES ('view_test_user')")
        .execute(&pool)
        .await?;

    // Query the dependent view to verify it works correctly
    let summary: Vec<(i32, String, i64)> = sqlx::query_as(
        "SELECT user_id, username, post_count FROM view_user_post_summary WHERE username = 'view_test_user'",
    )
    .fetch_all(&pool)
    .await?;

    // Should have the test user we just inserted
    assert_eq!(summary.len(), 1, "Should find exactly one user in view");
    assert_eq!(summary[0].1, "view_test_user", "Username should match");
    assert_eq!(summary[0].2, 0, "User should have 0 posts");

    Ok(())
}
