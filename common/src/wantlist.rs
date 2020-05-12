#[derive(Serialize, Deserialize, Debug)]
pub struct JsonCID {
    #[serde(rename = "/")]
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Entry {
    #[serde(alias = "Priority")]
    pub priority: i32,
    #[serde(alias = "Cancel")]
    pub cancel: bool,
    #[serde(alias = "SendDontHave")]
    pub send_dont_have: bool,
    #[serde(alias = "Cid")]
    pub cid: JsonCID,
    #[serde(alias = "WantType")]
    pub want_type: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IncrementalWantList {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub peer: String,
    pub received_entries: Option<Vec<Entry>>,
    pub full_want_list: Option<bool>,
    pub peer_connected: Option<bool>,
    pub peer_disconnected: Option<bool>,
    pub connect_event_peer_found: Option<bool>,
}
