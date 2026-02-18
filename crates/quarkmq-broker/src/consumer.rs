use quarkmq_protocol::rpc::ConsumerId;

#[derive(Debug, Clone)]
pub struct Consumer {
    pub id: ConsumerId,
    pub inflight_count: usize,
    pub max_inflight: usize,
}

impl Consumer {
    pub fn new(id: ConsumerId, max_inflight: usize) -> Self {
        Self {
            id,
            inflight_count: 0,
            max_inflight,
        }
    }

    pub fn can_accept(&self) -> bool {
        self.inflight_count < self.max_inflight
    }
}
