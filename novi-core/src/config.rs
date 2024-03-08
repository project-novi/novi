use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
#[serde(default)]
pub struct NoviConfig {
    pub tag_analyze_interval: u64,

    pub model_path: PathBuf,
}

impl Default for NoviConfig {
    fn default() -> Self {
        Self {
            tag_analyze_interval: 2 * 60,

            model_path: "models".into(),
        }
    }
}
