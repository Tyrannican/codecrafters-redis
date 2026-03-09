use crate::redis::protocol::Value;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

fn hasher(incoming: impl AsRef<[u8]>) -> Bytes {
    let mut sha = Sha256::new();
    sha.update(incoming.as_ref());
    hex::encode(sha.finalize()).into()
}

#[derive(Debug)]
pub struct UserStore {
    users: HashMap<Bytes, RedisUser>,
}

impl UserStore {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert(Bytes::from_static(&b"default"[..]), RedisUser::default());

        Self { users }
    }

    pub fn get(&self, user: &Bytes) -> Option<Value> {
        match self.users.get(user) {
            Some(user) => Some(user.repr()),
            None => None,
        }
    }

    pub fn set_password(&mut self, user: &Bytes, pass: &Bytes, client_id: &Bytes) {
        if let Some(user) = self.users.get_mut(user) {
            user.set_password(pass, client_id);
        }
    }

    pub fn authenticate(&mut self, user: &Bytes, pass: &Bytes, client_id: &Bytes) -> bool {
        match self.users.get_mut(user) {
            Some(user) => user.authenticate(pass, client_id),
            None => false,
        }
    }

    pub fn requires_authentication(&self, user: &Bytes, client_id: &Bytes) -> bool {
        match self.users.get(user) {
            Some(user) => user.requires_authentication(client_id),
            None => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisUser {
    flags: HashSet<UserFlag>,
    passwords: HashSet<Bytes>,
    authed_clients: HashSet<Bytes>,
}

impl RedisUser {
    pub fn set_password(&mut self, pass: &Bytes, client_id: &Bytes) {
        let pass = hasher(pass);
        self.passwords.insert(pass);
        self.authed_clients.insert(client_id.clone());
        self.flags.remove(&UserFlag::NoPass);
    }

    pub fn requires_authentication(&self, client_id: &Bytes) -> bool {
        !self.flags.contains(&UserFlag::NoPass) && !self.authed_clients.contains(client_id)
    }

    pub fn authenticate(&mut self, pass: &Bytes, client_id: &Bytes) -> bool {
        let pass = hasher(pass);
        if self.passwords.contains(&pass) {
            self.authed_clients.insert(client_id.clone());
            return true;
        }

        false
    }

    pub fn repr(&self) -> Value {
        let mut values: Vec<Value> = vec![Value::String("flags".into())];
        let flags = self
            .flags
            .iter()
            .map(|flag| Value::String(flag.to_string().into()))
            .collect::<Vec<Value>>();

        values.push(Value::Array(flags));
        values.push(Value::String("passwords".into()));

        let passwords = self
            .passwords
            .iter()
            .map(|pass| Value::String(pass.clone()))
            .collect::<Vec<Value>>();

        values.push(Value::Array(passwords));

        Value::Array(values)
    }
}

impl Default for RedisUser {
    fn default() -> Self {
        Self {
            flags: HashSet::from_iter([UserFlag::NoPass].into_iter()),
            passwords: HashSet::new(),
            authed_clients: HashSet::new(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum UserFlag {
    NoPass,
}

impl ToString for UserFlag {
    fn to_string(&self) -> String {
        match self {
            Self::NoPass => "nopass".to_string(),
        }
    }
}
