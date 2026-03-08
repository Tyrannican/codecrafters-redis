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

    pub fn get(&self, user: &Bytes) -> Option<RedisUser> {
        match self.users.get(user) {
            Some(user) => Some(user.clone()),
            None => None,
        }
    }

    pub fn set_password(&mut self, user: &Bytes, pass: &Bytes) {
        if let Some(user) = self.users.get_mut(user) {
            user.add_password(pass);
        }
    }

    pub fn authenticate(&self, user: &Bytes, pass: &Bytes) -> bool {
        match self.users.get(user) {
            Some(user) => user.authenticate(pass),
            None => false,
        }
    }

    pub fn requires_authentication(&self, user: &Bytes) -> bool {
        match self.users.get(user) {
            Some(user) => user.requires_authentication(),
            None => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisUser {
    username: Bytes,
    flags: HashSet<UserFlag>,
    passwords: HashSet<Bytes>,
}

impl RedisUser {
    pub fn add_password(&mut self, pass: &Bytes) {
        let pass = hasher(pass);
        self.passwords.insert(pass);
        self.flags.remove(&UserFlag::NoPass);
    }

    pub fn requires_authentication(&self) -> bool {
        !self.flags.contains(&UserFlag::NoPass)
    }

    pub fn authenticate(&self, pass: &Bytes) -> bool {
        let pass = hasher(pass);
        self.passwords.contains(&pass)
    }
}

impl Default for RedisUser {
    fn default() -> Self {
        Self {
            username: Bytes::from_static(&b"default"[..]),
            flags: HashSet::from_iter([UserFlag::NoPass].into_iter()),
            passwords: HashSet::new(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum UserFlag {
    NoPass,
}
