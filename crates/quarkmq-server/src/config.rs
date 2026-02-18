use clap::Parser;
use quarkmq_broker::BrokerConfig;
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(name = "quarkmq", about = "QuarkMQ - Kafka-compatible message queue")]
pub struct CliArgs {
    /// Path to config file
    #[arg(short, long, default_value = "config/quarkmq.toml")]
    pub config: String,

    /// Bind address
    #[arg(long)]
    pub bind: Option<String>,

    /// Data directory
    #[arg(long)]
    pub data_dir: Option<String>,

    /// Node ID
    #[arg(long)]
    pub node_id: Option<i32>,
}

#[derive(Debug, Deserialize, Default)]
pub struct FileConfig {
    #[serde(default)]
    pub broker: BrokerSection,
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub log: LogSection,
    #[serde(default)]
    pub defaults: DefaultsSection,
}

#[derive(Debug, Deserialize)]
pub struct BrokerSection {
    #[serde(default)]
    pub node_id: i32,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
}

impl Default for BrokerSection {
    fn default() -> Self {
        Self {
            node_id: 0,
            data_dir: default_data_dir(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ServerSection {
    #[serde(default = "default_bind")]
    pub bind: String,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            bind: default_bind(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LogSection {
    #[serde(default = "default_segment_bytes")]
    pub segment_bytes: u64,
    #[serde(default = "default_index_interval_bytes")]
    pub index_interval_bytes: u64,
}

impl Default for LogSection {
    fn default() -> Self {
        Self {
            segment_bytes: default_segment_bytes(),
            index_interval_bytes: default_index_interval_bytes(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DefaultsSection {
    #[serde(default = "default_num_partitions")]
    pub num_partitions: i32,
}

impl Default for DefaultsSection {
    fn default() -> Self {
        Self {
            num_partitions: default_num_partitions(),
        }
    }
}

fn default_data_dir() -> String {
    "/tmp/quarkmq/data".into()
}
fn default_bind() -> String {
    "0.0.0.0:9092".into()
}
fn default_segment_bytes() -> u64 {
    1073741824
}
fn default_index_interval_bytes() -> u64 {
    4096
}
fn default_num_partitions() -> i32 {
    1
}

pub struct ServerConfig {
    pub bind: String,
    pub broker_config: BrokerConfig,
    pub default_num_partitions: i32,
}

impl ServerConfig {
    pub fn load(args: &CliArgs) -> anyhow::Result<Self> {
        let file_config = if std::path::Path::new(&args.config).exists() {
            let contents = std::fs::read_to_string(&args.config)?;
            toml::from_str::<FileConfig>(&contents)?
        } else {
            FileConfig::default()
        };

        let bind = args
            .bind
            .clone()
            .or_else(|| std::env::var("QUARKMQ_BIND").ok())
            .unwrap_or(file_config.server.bind);

        let data_dir = args
            .data_dir
            .clone()
            .or_else(|| std::env::var("QUARKMQ_DATA_DIR").ok())
            .unwrap_or(file_config.broker.data_dir);

        let node_id = args
            .node_id
            .or_else(|| {
                std::env::var("QUARKMQ_NODE_ID")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(file_config.broker.node_id);

        let broker_config = BrokerConfig {
            node_id,
            data_dir,
            segment_bytes: file_config.log.segment_bytes,
            index_interval_bytes: file_config.log.index_interval_bytes,
            default_num_partitions: file_config.defaults.num_partitions,
        };

        Ok(Self {
            bind,
            broker_config,
            default_num_partitions: file_config.defaults.num_partitions,
        })
    }
}
