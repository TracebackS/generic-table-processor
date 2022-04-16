use by_address::ByAddress;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

/// Record's attributes, with it's type auto detected:
///   if it can be parsed as a int, then it's type is i32
///   else if it can be parsed as a float, then it's type is f32
///   else if it is `true` or `false`, then it's type is bool
///   else it is raw String
///
/// Attributes' types will be determined at the first time parsed, if conflicts detected shortly
/// after, an error msg will be emitted
///
/// It is small enough to be copyable
pub enum Attr {
    Int(i32),
    Float(f32),
    Bool(bool),
    Str(String),
}

/// Data record, looks up attribute's value by name
pub struct Record {
    attrs: HashMap<String, Attr>,
    group_id: u64,
}

impl Record {
    /// raw_record: vector of (header, value)
    pub fn new(ctx: &Ctx, raw_record: Vec<(&str, &str)>) -> Self {
        let mut attrs: HashMap<String, Attr> = HashMap::new();
        raw_record.into_iter().for_each(|e| {
            let (header, raw_attr) = e;
            let attr = match ctx.attr_type.get(header).expect(&format!(
                "Error: header `{}' is not found in context info",
                header
            )) {
                Attr::Int(_) => Attr::Int(raw_attr.parse::<i32>().expect(&format!(
                    "Error: expect int when parsing attribute {}, which value is {}",
                    header, raw_attr
                ))),
                Attr::Float(_) => Attr::Float(raw_attr.parse::<f32>().expect(&format!(
                    "Error: expect float when parsing attribute {}, which value is {}",
                    header, raw_attr
                ))),
                Attr::Bool(_) => Attr::Bool(match raw_attr {
                    "true" | "True" | "TRUE" | "t" | "T" => true,
                    "false" | "False" | "FALSE" | "f" | "F" => false,
                    _ => panic!(
                        "Error: expect bool when parsing attribute {}, which value is {}",
                        header, raw_attr
                    ),
                }),
                Attr::Str(_) => Attr::Str(raw_attr.into()),
            };
            attrs.insert(header.into(), attr);
        });

        // Hash the group id by rule
        let mut hasher = DefaultHasher::new();
        ctx.group_by.iter().for_each(|e| {
            let (attr_name, rule) = e;
            match attrs
                .get(attr_name)
                .expect("Error: key attribute is not found")
            {
                Attr::Int(v) => match rule {
                    ComponentRule::Unique => v.hash(&mut hasher),
                    ComponentRule::Interval(interval) => {
                        ((v - interval.start) / interval.step).hash(&mut hasher)
                    }
                },
                Attr::Float(v) => match rule {
                    ComponentRule::Unique => (v.trunc() as i32).hash(&mut hasher),
                    ComponentRule::Interval(interval) => {
                        (((v.trunc() as i32) - interval.start) / interval.step).hash(&mut hasher)
                    }
                },
                Attr::Bool(v) => v.hash(&mut hasher),
                Attr::Str(v) => v.hash(&mut hasher),
            }
        });
        let group_id = hasher.finish();

        Record { attrs, group_id }
    }
}

pub struct Interval {
    start: i32,
    step: i32,
}

pub enum ComponentRule {
    Unique,
    Interval(Interval),
}

/// Set context:
///   attributes' types
///   definition of group by
pub struct Ctx {
    attr_type: HashMap<String, Attr>,
    group_by: HashMap<String, ComponentRule>,
}

impl Ctx {
    pub fn new() -> Self {
        Ctx {
            attr_type: HashMap::new(),
            group_by: HashMap::new(),
        }
    }

    pub fn add_attr_type(
        &mut self,
        attr_name: &str,
        attr_type: Attr,
        group_by: Option<ComponentRule>,
    ) {
        self.attr_type.insert(attr_name.into(), attr_type);
        if let Some(group_by_component) = group_by {
            self.group_by.insert(attr_name.into(), group_by_component);
        }
    }
}

/// A group is a set of Record with same `group_id`s. Records are never changed, so here stores
/// `Record`s references
#[derive(Clone)]
pub struct Group<'a> {
    records: HashSet<ByAddress<&'a Record>>,
    id: u64,
}

impl<'a> Group<'a> {
    fn new(id: u64) -> Self {
        Self {
            records: HashSet::new(),
            id,
        }
    }
}

/// A Collection is a set of groups, with full or part of records in them.
#[derive(Clone)]
pub struct Collection<'a> {
    groups: HashMap<u64, Group<'a>>,
}

impl<'a> Collection<'a> {
    pub fn new(records: Vec<&'a Record>) -> Self {
        let mut groups: HashMap<u64, Group> = HashMap::new();
        records.into_iter().for_each(|record| {
            if !groups.contains_key(&record.group_id) {
                groups.insert(record.group_id, Group::new(record.group_id));
            }
            groups
                .get_mut(&record.group_id)
                .expect("Impossible: key is not found when creating new collection")
                .records
                .insert(ByAddress(&record));
        });
        Self { groups }
    }

    /// Filter the collection with predicate, generate new collection
    // TODO
    pub fn filter_records(&self /* TODO: filter cond */) -> Self {
        let groups = self
            .groups
            .iter()
            .map(|e| {
                let (id, group) = e;
                let records: HashSet<ByAddress<&Record>> = group
                    .records
                    .clone()
                    .into_iter()
                    .filter(|_| true /* TODO: filter cond */)
                    .collect();
                (
                    id.to_owned(),
                    Group {
                        records,
                        id: id.to_owned(),
                    },
                )
            })
            .collect();
        Self { groups }
    }

    // TODO
    pub fn intersect(&self, other: &Self) -> Self {
        self.clone()
    }

    // TODO
    pub fn unite(&self, other: &Self) -> Self {
        self.clone()
    }

    // TODO
    pub fn differ(&self, other: &Self) -> Self {
        self.clone()
    }

    // Handle fold operation
    pub fn fold(&self, op: FoldOperation) -> FoldResult {
        match op {
            FoldOperation::AVG(attr_name) => self.avg(&attr_name),
            FoldOperation::SUM(attr_name) => self.sum(&attr_name),
            FoldOperation::COUNT => self.count(),
        }
    }

    fn avg(&self, attr_name: &str) -> FoldResult {
        let result: HashMap<ByAddress<&Group>, Attr> = self
            .groups
            .iter()
            .map(|e| {
                let (_, group) = e;
                let (sum, count) = group.records.iter().fold((0f32, 0i32), |acc, x| {
                    let (mut sum, mut count) = acc;
                    sum += match x.attrs.get(attr_name).unwrap_or(&Attr::Float(0f32)) {
                        Attr::Int(v) => v.to_owned() as f32,
                        Attr::Float(v) => v.to_owned(),
                        _ => panic!("AVG operation should be performed on int or float"),
                    };
                    count += 1;
                    (sum, count)
                });
                (ByAddress(group), Attr::Float(sum / (count as f32)))
            })
            .collect();
        FoldResult {
            collection: ByAddress(self),
            fold_func: FoldOperation::AVG(attr_name.into()),
            result,
        }
    }

    fn sum(&self, attr_name: &str) -> FoldResult {
        let result: HashMap<ByAddress<&Group>, Attr> = self
            .groups
            .iter()
            .map(|e| {
                let (_, group) = e;
                let sum = group.records.iter().fold(0f32, |acc, x| {
                    acc + match x.attrs.get(attr_name).unwrap_or(&Attr::Float(0f32)) {
                        Attr::Int(v) => v.to_owned() as f32,
                        Attr::Float(v) => v.to_owned(),
                        _ => panic!("AVG operation should be performed on int or float"),
                    }
                });
                (ByAddress(group), Attr::Float(sum))
            })
            .collect();
        FoldResult {
            collection: ByAddress(self),
            fold_func: FoldOperation::SUM(attr_name.into()),
            result,
        }
    }

    fn count(&self) -> FoldResult {
        let result: HashMap<ByAddress<&Group>, Attr> = self
            .groups
            .iter()
            .map(|e| {
                let (_, group) = e;
                let count = group.records.iter().count();
                (ByAddress(group), Attr::Int(count as i32))
            })
            .collect();
        FoldResult {
            collection: ByAddress(self),
            fold_func: FoldOperation::COUNT,
            result,
        }
    }
}

pub enum FoldOperation {
    AVG(String), // AVG of attr
    SUM(String), // SUM of attr
    COUNT,       // items count
}

/// FoldResult is binding to collection and fold_func, and mapping each group to a scalar result
pub struct FoldResult<'a> {
    collection: ByAddress<&'a Collection<'a>>,
    fold_func: FoldOperation,
    result: HashMap<ByAddress<&'a Group<'a>>, Attr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_collection() {
        assert_eq!(1, 1)
    }
}
