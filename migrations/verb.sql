CREATE DATABASE IF NOT EXISTS verb;

CREATE TABLE IF NOT EXISTS verb.task (
    id varchar(32) primary key COMMENT 'unique id',
    name varchar(128) NOT NULL DEFAULT "" COMMENT "",
    last_heartbeat bigint NOT NULL DEFAULT '0' COMMENT '',
    src_type varchar(32) NOT NULL DEFAULT '' COMMENT '',
    dst_type varchar(32) NOT NULL DEFAULT '' COMMENT '',
    src_cfg TEXT DEFAULT NULL COMMENT '',
    dst_cfg TEXT DEFAULT NULL COMMENT '',
    tasking_cfg TEXT DEFAULT NULL COMMENT '',
    debug_text TEXT DEFAULT NULL COMMENT 'debug text json format',
    properties TEXT DEFAULT NULL COMMENT 'node property',
    status tinyint NOT NULL DEFAULT '0' COMMENT "task status has 0:",
    created_at bigint NOT NULL DEFAULT '0' COMMENT 'created timestamp',
    updated_at bigint NOT NULL DEFAULT '0' COMMENT 'updated  timestamp',
    deleted_at bigint NOT NULL DEFAULT '0' COMMENT 'deleted  timestamp'
) engine = innodb COMMENT 'task info';