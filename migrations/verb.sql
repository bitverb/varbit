CREATE DATABASE IF NOT EXISTS verb;

CREATE TABLE IF NOT EXISTS verb.task (
    id varchar(32) primary key COMMENT 'unique id',
    name varchar(128) NOT NULL DEFAULT "" COMMENT "",
    last_heartbeat bigint NOT NULL DEFAULT '0' COMMENT '',
    src_type varchar(32) NOT NULL DEFAULT '' COMMENT '',
    dst_type varchar(32) NOT NULL DEFAULT '' COMMENT '',
    src_cfg JSON DEFAULT NULL COMMENT '',
    dst_cfg JSON DEFAULT NULL COMMENT '',
    tasking_cfg JSON DEFAULT NULL COMMENT '',
    status tinyint NOT NULL DEFAULT '0' COMMENT "task status has 0:",
    created_at bigint NOT NULL DEFAULT '0' COMMENT 'created timestamp',
    updated_at bigint NOT NULL DEFAULT '0' COMMENT 'updated  timestamp',
    deleted_at bigint NOT NULL DEFAULT '0' COMMENT 'updated  timestamp'
) engine = innodb COMMENT 'task info';