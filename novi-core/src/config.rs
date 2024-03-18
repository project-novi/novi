use serde::Deserialize;
#[derive(Deserialize)]
#[serde(default)]
pub struct NoviConfig {
    pub session_key: String,
}

impl Default for NoviConfig {
    fn default() -> Self {
        Self {
            session_key: "testkey".to_owned(),
        }
    }
}
