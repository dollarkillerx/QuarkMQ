use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub channels: ChannelDefaults,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    #[serde(default = "default_node_id")]
    pub id: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_ws_bind")]
    pub ws_bind: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct ChannelDefaults {
    #[serde(default = "default_ack_timeout")]
    pub ack_timeout_secs: u64,
    #[serde(default = "default_max_attempts")]
    pub max_delivery_attempts: u32,
    #[serde(default = "default_max_inflight")]
    pub max_inflight_per_consumer: usize,
}

fn default_node_id() -> String {
    "node-1".to_string()
}

fn default_data_dir() -> String {
    "/var/lib/quarkmq/data".to_string()
}

fn default_ws_bind() -> String {
    "0.0.0.0:9876".to_string()
}

fn default_ack_timeout() -> u64 {
    30
}

fn default_max_attempts() -> u32 {
    5
}

fn default_max_inflight() -> usize {
    100
}

#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            server: ServerConfig::default(),
            channels: ChannelDefaults::default(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: default_node_id(),
            data_dir: default_data_dir(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            ws_bind: default_ws_bind(),
        }
    }
}

impl Default for ChannelDefaults {
    fn default() -> Self {
        Self {
            ack_timeout_secs: default_ack_timeout(),
            max_delivery_attempts: default_max_attempts(),
            max_inflight_per_consumer: default_max_inflight(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        if let Some(path) = path {
            let content = std::fs::read_to_string(path)?;
            let config: Config = toml::from_str(&content)?;
            Ok(config)
        } else {
            Ok(Config::default())
        }
    }
}
