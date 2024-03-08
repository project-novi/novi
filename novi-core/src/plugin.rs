use serde::Deserialize;
use std::{path::PathBuf, process};

#[derive(Deserialize)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub authors: Vec<String>,
    pub license: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default)]
    pub namespaces: Vec<String>,
}

pub(crate) struct PluginState {
    pub info: PluginInfo,
    pub path: PathBuf,
    pub process: process::Child,
}
