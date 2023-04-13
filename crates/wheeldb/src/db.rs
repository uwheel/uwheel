use rocksdb::{merge_operator::MergeFn, ColumnFamilyDescriptor, Options, DB};
use smallvec::SmallVec;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    path::PathBuf,
    rc::Rc,
};

use crate::lsm::*;

pub type Key = SmallVec<[u8; 16]>;

pub struct WheelDB {
    wheels: HashMap<Key, Wheel>,
    db: Rc<RefCell<DB>>,
}

impl WheelDB {
    pub fn open_default(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let column_families: HashSet<String> = match DB::list_cf(&opts, &path) {
            Ok(cfs) => cfs.into_iter().filter(|n| n != "default").collect(),
            // TODO: possibly platform-dependant error message check
            Err(e) if e.to_string().contains("No such file or directory") => HashSet::new(),
            //Err(e) => return Err(e.into()),
            Err(_) => panic!("fix"),
        };

        let cfds = if !column_families.is_empty() {
            column_families
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect()
        } else {
            vec![ColumnFamilyDescriptor::new("default", Options::default())]
        };
        let db = DB::open_cf_descriptors(&opts, &path, cfds).unwrap();
        Self {
            wheels: Default::default(),
            db: Rc::new(RefCell::new(db)),
        }
    }
    pub fn wheel(&self, name: impl AsRef<[u8]>) -> Option<&Wheel> {
        self.wheels.get(name.as_ref())
    }
    pub fn create_wheel(
        &mut self,
        name: impl Into<String>,
        merge_fn: impl MergeFn + Clone + 'static,
    ) {
        let wheel_cf = name.into();
        let wheel = Wheel::new(&wheel_cf, self.db.clone(), 0, merge_fn);
        self.wheels
            .insert(Key::from_slice(wheel_cf.as_bytes()), wheel);
    }
    pub fn delete_wheel(&self, _name: impl Into<String>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::MergeOperands;
    use time::ext::NumericalDuration;
    pub fn concat_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Vec<u8> = Vec::with_capacity(operands.len());
        if let Some(v) = existing_val {
            for e in v {
                result.push(*e)
            }
        }
        for op in operands {
            for e in op {
                result.push(*e)
            }
        }
        Some(result)
    }

    #[test]
    fn db_test() {
        let mut db = WheelDB::open_default("/tmp/wheeldb");

        db.create_wheel("test_wheel", concat_merge);

        db.wheel("test_wheel")
            .unwrap()
            .insert("hej", 1u32.to_le_bytes(), 1000);

        let v = db
            .wheel("test_wheel")
            .unwrap()
            .seconds_wheel()
            .read()
            .get("hej", 0);

        assert_eq!(v, None);

        db.wheel("test_wheel").unwrap().advance(1.seconds());

        let v = db
            .wheel("test_wheel")
            .unwrap()
            .seconds_wheel()
            .read()
            .get("hej", 0);

        assert!(v.is_some());
    }
}
