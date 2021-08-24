use byteorder::{BigEndian, ByteOrder};
use rocksdb::{DBWithThreadMode, Error, Options, SingleThreaded, DB};

pub struct RocksDBStore {
    db: DBWithThreadMode<SingleThreaded>,
    options: Options,
}

fn empty_chain_ids(_: Error) -> Result<Vec<ChainId>, Error> {
    Ok(vec![])
}

impl RocksDBStore {
    pub fn new(path: &str) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);

        let cfs = DB::list_cf(&options, path).or_else(empty_chain_ids).unwrap();
        let db = DB::open_cf(&options, path, cfs)?;
        Ok(Self { db, options })
    }
}

type Address = Vec<u8>;
type ChainId = String;

pub trait ChainDB {
    fn chain_ids(&self) -> Result<Vec<ChainId>, Error>;
    fn get_tx_nonce(&self, address: &Address, chain_id: &ChainId) -> Result<u64, Error>;
}

impl ChainDB for RocksDBStore {
    fn chain_ids(&self) -> Result<Vec<ChainId>, Error> {
        DB::list_cf(&self.options, self.db.path())
    }

    fn get_tx_nonce(&self, address: &Address, chain_id: &ChainId) -> Result<u64, Error> {
        let cf_handle = self.db.cf_handle(chain_id);
        if let Some(bytes) = self.db.get_cf(cf_handle.unwrap(), address)? {
            Ok(BigEndian::read_u64(&bytes))
        } else {
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs::create_dir;
    use super::{ChainDB, RocksDBStore};

    #[test]
    fn create() -> () {
        let temp_dir = env::temp_dir();
        let temp_dir = temp_dir.join("temporary");
        if !temp_dir.is_dir() {
            create_dir(temp_dir.to_owned());
        }

        let temp_dir = temp_dir.to_str().unwrap();        
        let db = RocksDBStore::new(&temp_dir).unwrap();
        assert_eq!(true, db.chain_ids().is_ok());
        
        let chain_ids = db.chain_ids().unwrap();
        assert_eq!(1, chain_ids.len());
        assert_eq!("default", chain_ids[0]);
    }
}
