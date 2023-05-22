use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct AuthMessage {
    pub encrypted_tags: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TagSetsSpecifier {
    pub tag_sets: Vec<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
pub struct JwtValidationMessage {
    pub r#type: String,
    pub success: bool,
    pub token: String,
}
