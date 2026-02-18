use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct BrokerConfig {
    pub node_id: i32,
    pub data_dir: String,
    #[serde(default = "default_segment_bytes")]
    pub segment_bytes: u64,
    #[serde(default = "default_index_interval_bytes")]
    pub index_interval_bytes: u64,
    #[serde(default = "default_num_partitions")]
    pub default_num_partitions: i32,
    #[serde(default = "default_advertised_host")]
    pub advertised_host: String,
    #[serde(default = "default_advertised_port")]
    pub advertised_port: i32,
}

fn default_segment_bytes() -> u64 {
    1_073_741_824 // 1GB
}

fn default_index_interval_bytes() -> u64 {
    4096
}

fn default_num_partitions() -> i32 {
    1
}

fn default_advertised_host() -> String {
    "localhost".into()
}

fn default_advertised_port() -> i32 {
    9092
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            data_dir: "/tmp/quarkmq/data".into(),
            segment_bytes: default_segment_bytes(),
            index_interval_bytes: default_index_interval_bytes(),
            default_num_partitions: default_num_partitions(),
            advertised_host: default_advertised_host(),
            advertised_port: default_advertised_port(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BrokerConfig::default();
        assert_eq!(config.node_id, 0);
        assert_eq!(config.data_dir, "/tmp/quarkmq/data");
        assert_eq!(config.segment_bytes, 1_073_741_824);
        assert_eq!(config.index_interval_bytes, 4096);
        assert_eq!(config.default_num_partitions, 1);
        assert_eq!(config.advertised_host, "localhost");
        assert_eq!(config.advertised_port, 9092);
    }

    #[test]
    fn test_deserialize_minimal() {
        let toml_str = r#"
            node_id = 1
            data_dir = "/var/quarkmq"
        "#;
        let config: BrokerConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.node_id, 1);
        assert_eq!(config.data_dir, "/var/quarkmq");
        // Defaults should be applied.
        assert_eq!(config.segment_bytes, 1_073_741_824);
        assert_eq!(config.index_interval_bytes, 4096);
        assert_eq!(config.default_num_partitions, 1);
    }

    #[test]
    fn test_deserialize_full() {
        let toml_str = r#"
            node_id = 2
            data_dir = "/data/quarkmq"
            segment_bytes = 536870912
            index_interval_bytes = 8192
            default_num_partitions = 4
        "#;
        let config: BrokerConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.node_id, 2);
        assert_eq!(config.data_dir, "/data/quarkmq");
        assert_eq!(config.segment_bytes, 536_870_912);
        assert_eq!(config.index_interval_bytes, 8192);
        assert_eq!(config.default_num_partitions, 4);
    }

    #[test]
    fn test_clone() {
        let config = BrokerConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.node_id, config.node_id);
        assert_eq!(cloned.data_dir, config.data_dir);
    }
}
