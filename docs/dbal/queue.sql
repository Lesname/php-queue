CREATE TABLE IF NOT EXISTS queue
(
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,

    job_name VARCHAR(50) NOT NULL,
    job_state VARCHAR(10) NOT NULL,
    job_message JSON NOT NULL,
    job_attempt TINYINT UNSIGNED NOT NULL DEFAULT 0,

    job_priority TINYINT NOT NULL,
    job_until INT(11) UNSIGNED,

    reserved_on INT(11) UNSIGNED DEFAULT NULL,
    reserved_release INT(11) UNSIGNED DEFAULT NULL,
    reserved_key VARCHAR(10) DEFAULT NULL,

    `v_checksum` char(32) GENERATED ALWAYS AS (md5(`job_message`)) VIRTUAL,

    INDEX reserve (
                   job_state,
                   job_until,
                   job_priority
        ),

    INDEX `check` (job_name, v_checksum),

    UNIQUE (reserved_key)
)
    CHARACTER SET = utf8mb4
    COLLATE utf8mb4_unicode_ci
    ENGINE = InnoDB;
