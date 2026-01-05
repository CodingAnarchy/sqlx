-- Migration to add explicit FK constraints for testing FK cloning
-- Note: In MySQL, column-level REFERENCES syntax is parsed but ignored.
-- Explicit FK constraints are needed for enforcement.

ALTER TABLE post
    ADD CONSTRAINT fk_post_user FOREIGN KEY (user_id) REFERENCES user(user_id) ON DELETE CASCADE;

ALTER TABLE comment
    ADD CONSTRAINT fk_comment_post FOREIGN KEY (post_id) REFERENCES post(post_id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_comment_user FOREIGN KEY (user_id) REFERENCES user(user_id) ON DELETE CASCADE;
