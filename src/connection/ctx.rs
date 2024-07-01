use kanal::AsyncSender;

// NOTE: Format is (master ip, master port, replica port)
pub type ReplicaMaster = (String, u16, u16);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRole {
    Master,
    Replica(ReplicaMaster),
}

impl ServerRole {
    pub fn as_string(&self) -> String {
        match *self {
            Self::Master => "master".to_string(),
            Self::Replica(_) => "slave".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerInformation {
    master_replid: String,
    master_repl_offset: usize,
}

impl ServerInformation {
    pub fn new() -> Self {
        Self {
            // TODO: Pass this in if required
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerContext {
    role: ServerRole,
    server_information: ServerInformation,
    replicas: Vec<AsyncSender<String>>,
}

impl ServerContext {
    pub fn new(role: ServerRole) -> Self {
        Self {
            role,
            server_information: ServerInformation::new(),
            replicas: vec![],
        }
    }

    pub fn _server_role(&self) -> ServerRole {
        self.role.clone()
    }

    pub fn server_information(&self) -> String {
        format!(
            "role:{}master_replid:{}master_repl_offset:{}",
            self.role.as_string(),
            self.server_information.master_replid,
            self.server_information.master_repl_offset
        )
    }

    pub fn replicas(&self) -> &Vec<AsyncSender<String>> {
        &self.replicas
    }

    pub fn server_replid(&self) -> String {
        self.server_information.master_replid.clone()
    }

    pub fn add_replica(&mut self, replica: AsyncSender<String>) {
        self.replicas.push(replica);
    }
}
