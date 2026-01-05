-- Create a standalone table for testing AUTO_INCREMENT behavior after template cloning.
-- Using a separate table avoids conflicts with fixture data in user/post/comment tables.
-- After cloning from a template, AUTO_INCREMENT should continue correctly
-- from MAX(id) + 1, not reset to 1 (which would cause duplicate key errors).

CREATE TABLE auto_increment_test (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL
);

INSERT INTO auto_increment_test (name) VALUES ('seed_row_1'), ('seed_row_2'), ('seed_row_3');
