// src/export.rs

use std::{path::PathBuf, sync::Arc};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Export {
    pub path: PathBuf,
    pub read_only: bool,
    pub anon_uid: u32,
    pub anon_gid: u32,
    pub clients: Vec<String>,
}

#[derive(Clone)]
pub struct Exports(Arc<Vec<Export>>);

impl Exports {
    pub fn new(v: Vec<Export>) -> Self {
        Self(Arc::new(v))
    }
    pub fn list(&self) -> &[Export] {
        &self.0
    }
    #[allow(dead_code)]
    pub fn by_path(&self, p: &str) -> Option<Export> {
        self.0
            .iter()
            .find(|e| e.path.to_string_lossy() == p)
            .cloned()
    }
}
