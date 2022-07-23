CREATE TABLE IF NOT EXISTS queue
(
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,

    job_name VARCHAR(50) NOT NULL,
    job_state VARCHAR(10) NOT NULL,
    job_data JSON NOT NULL,
    job_attempt TINYINT UNSIGNED NOT NULL DEFAULT 0,

    job_priority TINYINT NOT NULL,
    job_until INT(11) UNSIGNED,

    reserved_on INT(11) UNSIGNED DEFAULT NULL,
    reserved_release INT(11) UNSIGNED DEFAULT NULL,
    reserved_key VARCHAR(10) DEFAULT NULL,

    INDEX reserve (
                   job_state,
                   job_until,
                   job_priority
        ),

    UNIQUE (reserved_key)
)
    CHARACTER SET = utf8mb4
    COLLATE utf8mb4_unicode_ci
    ENGINE = InnoDB;
