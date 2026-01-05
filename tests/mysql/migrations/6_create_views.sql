-- Create views with dependencies to test view cloning order.
-- view_user_posts depends on base tables.
-- view_user_post_summary depends on view_user_posts (nested view dependency).
-- Template cloning must handle the dependency order correctly.

CREATE VIEW view_user_posts AS
SELECT u.user_id, u.username, p.post_id, p.content as post_content
FROM user u
LEFT JOIN post p ON u.user_id = p.user_id;

CREATE VIEW view_user_post_summary AS
SELECT user_id, username, COUNT(post_id) as post_count
FROM view_user_posts
GROUP BY user_id, username;
