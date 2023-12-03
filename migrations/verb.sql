CREATE DATABASE IF NOT EXISTS verb;

CREATE TABLE verb.task IF NOT EXISTS (
    id varchar(32) primary key COMMENT 'unique id',
    name varchar(128) NOT NULL DEFAULT "" COMMENT "",
    status tinyint NOT NULL DEFAULT '0' COMMENT "task status has 0:",
    created_at bigint NOT NULL DEFAULT '0' COMMENT 'created timestamp',
    updated_at bigint NOT NULL DEFAULT '0' COMMENT 'updated  timestamp'
)engine=innodb COMMENT 'task info';