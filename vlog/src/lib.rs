/// log mod
use chrono::Local;
use env_logger::fmt::{Formatter, self};
use log::Record;
use std::io;
use std::io::Write;

fn text_logger_formatter(buf: &mut Formatter, record: &Record) -> io::Result<()> {
    return writeln!(
        buf,
        "{} [{}] path={}, file={}, line={}, msg={}",
        Local::now().format("%Y-%m-%d %H:%M:%S.%f"),
        record.level(),
        record.module_path().unwrap(),
        record.file().unwrap(),
        record.line().unwrap(),
        record.args(),
    );
}

pub fn init_log(output:fmt::Target,level: log::LevelFilter) {
    env_logger::builder()
        //log format
        .format(text_logger_formatter)
        // log level
        .filter_level(level)
        // output
        .target(output)
        //
        .init();
}
