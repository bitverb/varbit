use std::fs;

use app::AppConfig;

pub mod app;

pub fn from_path(path: String) -> AppConfig {
    let config_txt: String = match fs::read_to_string(path.to_owned()) {
        Ok(txt) => txt,
        Err(err) => {
            panic!("unable to read file {}", err)
        }
    };

    // decode as AppConfig
    match toml::from_str(&config_txt) {
        Ok(v) => v,
        Err(err) => {
            panic!(
                "unable to   decode [{}.content] as conf {}",
                path.to_owned(),
                err
            )
        }
    }
}
