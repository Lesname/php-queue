CREATE TABLE IF NOT EXISTS queue_job
(
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,

    name VARCHAR(50) NOT NULL,
    state VARCHAR(10) NOT NULL,
    data TEXT NOT NULL,
    attempt TINYINT UNSIGNED NOT NULL DEFAULT 0,
    until INT(11) UNSIGNED,
    priority TINYINT UNSIGNED,

    reserved_on INT(11) UNSIGNED DEFAULT NULL,
    reserved_release INT(11) UNSIGNED DEFAULT NULL,
    reserved_key VARCHAR(10) DEFAULT NULL,

    INDEX reserve (state, until),

    UNIQUE (reserved_key)
)
    CHARACTER SET = utf8mb4
    COLLATE utf8mb4_unicode_ci
    ENGINE = InnoDB;
