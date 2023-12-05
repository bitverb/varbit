/// pubg mod is plugin
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub mod input;
pub mod sink;

use input::kafka::{KafkaSrc, Src};
use lazy_static::lazy_static;

lazy_static! {
    /// link https://users.rust-lang.org/t/how-to-add-a-trait-value-into-hashmap/6542/3
    /// hashmap add dyn trait
    pub static ref SRC_PLUGIN: Arc<Mutex<HashMap<String,Box<dyn Src  +Send +Sync>>>> =   {
        let mut plugin :HashMap<String,Box<dyn Src  +Send +Sync>>= HashMap::new();
        plugin.insert(String::from("kafka"), Box::new(KafkaSrc{}));
        Arc::new(Mutex::new(plugin))
    };

}
