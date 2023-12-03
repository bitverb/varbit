/// logger mod
use chrono::Local;
use log::Record;
use std::io::Write;
use std::io;
use env_logger::fmt;
use env_logger::fmt::Formatter;

// logger formatter
fn text_logger_formatter(buf: &mut Formatter, record: &Record) -> io::Result<()> {
    return writeln!(
        buf,
        "{} [{}] path={}, file={}, line={}, msg={:?}",
        // time format
        Local::now().format("%Y-%m-%d %H:%M:%S.%f"),
        record.level(),
        record.module_path().unwrap(),
        record.file().unwrap(),
        record.line().unwrap(),
        record.args(),
    );
}

pub fn init_log(output: fmt::Target) {
    env_logger::builder()
        //log formatter
        .format(text_logger_formatter)
        // level
        .filter_level(log::LevelFilter::Debug)
        // output
        .target(output)
        // build logger
        .init();
}