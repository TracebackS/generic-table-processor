use by_address::ByAddress;
use std::cmp::{Ordering, PartialOrd};
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
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
#[derive(PartialEq, PartialOrd, Debug)]
pub enum Attr {
    Int(i32),
    Float(f32),
    Bool(bool),
    Str(String),
}

impl Attr {
    fn new(ctx: &Ctx, header: &str, raw_attr: &str) -> Self {
        match ctx.attr_type.get(header).expect(&format!(
            "Error: header `{}' is not found in context info",
            header
        )) {
            Attr::Int(_) => Attr::Int(raw_attr.parse::<i32>().expect(&format!(
                "Error: expect int when parsing attribute `{}', which value is `{}'",
                header, raw_attr
            ))),
            Attr::Float(_) => Attr::Float(raw_attr.parse::<f32>().expect(&format!(
                "Error: expect float when parsing attribute `{}', which value is `{}'",
                header, raw_attr
            ))),
            Attr::Bool(_) => Attr::Bool(match raw_attr {
                "true" | "True" | "TRUE" | "t" | "T" => true,
                "false" | "False" | "FALSE" | "f" | "F" => false,
                _ => panic!(
                    "Error: expect bool when parsing attribute `{}', which value is `{}'",
                    header, raw_attr
                ),
            }),
            Attr::Str(_) => Attr::Str(raw_attr.into()),
        }
    }
}

/// Data record, looks up attribute's value by name
pub struct Record {
    attrs: HashMap<String, Attr>,
    group_id: u64,
}

impl Record {
    /// raw_record: vector of (header, value)
    pub fn new(ctx: &Ctx, raw_record: Vec<(&str, &str)>) -> Self {
        let attrs: HashMap<String, Attr> = raw_record
            .into_iter()
            .map(|(header, raw_attr)| (header.into(), Attr::new(ctx, header, raw_attr)))
            .collect();

        // Hash the group id by rule
        let mut hasher = DefaultHasher::new();
        ctx.group_by.iter().for_each(|(attr_name, rule)| {
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

// TODO: This implementation is temporary
pub struct FilterCond {
    attr_name: String,
    val: Attr,
    ord: Ordering,
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
    // TODO: Filter cond need reimplementation
    pub fn filter_records(mut self, filter_cond: FilterCond) -> Self {
        self.groups = self
            .groups
            .into_iter()
            .filter_map(|(id, group)| {
                let records: HashSet<ByAddress<&Record>> = group
                    .records
                    .into_iter()
                    .filter(|record| {
                        record
                            .attrs
                            .get(&filter_cond.attr_name)
                            .partial_cmp(&Some(&filter_cond.val))
                            == Some(filter_cond.ord)
                    })
                    .collect();
                if records.is_empty() {
                    None
                } else {
                    Some((
                        id.to_owned(),
                        Group {
                            records,
                            id: id.to_owned(),
                        },
                    ))
                }
            })
            .collect();
        self
    }

    pub fn intersection(mut self, other: &Self) -> Self {
        self.groups = self
            .groups
            .into_iter()
            .filter_map(|(id, group)| {
                if let Some(other_group) = other.groups.get(&id) {
                    let records: HashSet<_> = group
                        .records
                        .intersection(&other_group.records)
                        .map(|x| x.to_owned())
                        .collect();
                    if records.is_empty() {
                        None
                    } else {
                        Some((
                            id.to_owned(),
                            Group {
                                records,
                                id: id.to_owned(),
                            },
                        ))
                    }
                } else {
                    None
                }
            })
            .collect();
        self
    }

    pub fn union(mut self, other: &Self) -> Self {
        other.groups.iter().for_each(|(id, other_group)| {
            if let Some(mut group) = self.groups.get_mut(id) {
                group.records = group
                    .records
                    .union(&other_group.records)
                    .map(|x| x.to_owned())
                    .collect();
            } else {
                self.groups.insert(id.to_owned(), other_group.to_owned());
            }
        });
        self
    }

    pub fn difference(mut self, other: &Self) -> Self {
        self.groups = self
            .groups
            .into_iter()
            .filter_map(|(id, group)| {
                if let Some(other_group) = other.groups.get(&id) {
                    let records: HashSet<_> = group
                        .records
                        .difference(&other_group.records)
                        .map(|x| x.to_owned())
                        .collect();
                    if records.is_empty() {
                        None
                    } else {
                        Some((
                            id.to_owned(),
                            Group {
                                records,
                                id: id.to_owned(),
                            },
                        ))
                    }
                } else {
                    None
                }
            })
            .collect();
        self
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
            .map(|(_, group)| {
                let (sum, count) = group.records.iter().fold((0f32, 0i32), |(sum, count), x| {
                    (
                        sum + match x.attrs.get(attr_name).unwrap_or(&Attr::Float(0f32)) {
                            Attr::Int(v) => v.to_owned() as f32,
                            Attr::Float(v) => v.to_owned(),
                            _ => panic!("AVG operation should be performed on int or float"),
                        },
                        count + 1,
                    )
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
            .map(|(_, group)| {
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
            .map(|(_, group)| {
                (
                    ByAddress(group),
                    Attr::Int(group.records.iter().count() as i32),
                )
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
    use std::iter::zip;

    fn make_a_ctx() -> Ctx {
        let mut ctx = Ctx::new();
        ctx.add_attr_type("userid", Attr::Int(0), Some(ComponentRule::Unique));
        ctx.add_attr_type(
            "time",
            Attr::Float(0f32),
            Some(ComponentRule::Interval(Interval { start: 1, step: 3 })),
        );
        ctx.add_attr_type("i", Attr::Int(0), None);
        ctx.add_attr_type("f", Attr::Float(0f32), None);
        ctx.add_attr_type("b", Attr::Bool(false), None);
        ctx.add_attr_type("s", Attr::Str("hello".into()), None);
        ctx
    }

    #[test]
    fn construct_record() {
        let headers = vec!["userid", "time", "i", "f", "b", "s"];
        let raw_record_a = vec!["23", "2", "0", ".23", "true", "hello"];
        let raw_record_b = vec!["23", "3", "8", ".45", "true", "world"];
        let raw_record_c = vec!["24", "2", "1", ".25", "false", "yeah"];
        let ctx = make_a_ctx();

        let record_a = Record::new(
            &ctx,
            zip(headers.iter(), raw_record_a.into_iter())
                .map(|(x, y)| (x.to_owned(), y))
                .collect(),
        );

        assert_eq!(record_a.attrs.get("userid"), Some(&Attr::Int(23)));
        assert_eq!(record_a.attrs.get("time"), Some(&Attr::Float(2f32)));
        assert_eq!(record_a.attrs.get("i"), Some(&Attr::Int(0)));
        assert_eq!(record_a.attrs.get("f"), Some(&Attr::Float(0.23f32)));
        assert_eq!(record_a.attrs.get("b"), Some(&Attr::Bool(true)));
        assert_eq!(record_a.attrs.get("x"), None);

        let record_b = Record::new(
            &ctx,
            zip(headers.iter(), raw_record_b.iter())
                .map(|(x, y)| (x.to_owned(), y.to_owned()))
                .collect(),
        );
        let record_c = Record::new(
            &ctx,
            zip(headers.iter(), raw_record_c.iter())
                .map(|(x, y)| (x.to_owned(), y.to_owned()))
                .collect(),
        );

        assert_eq!(record_a.group_id, record_b.group_id);
        assert_ne!(record_a.group_id, record_c.group_id);
    }

    #[test]
    fn group_record() {
        let headers = vec!["userid", "time"];
        let ctx = make_a_ctx();
        let record_group_a = vec![
            vec!["0", "1.1"],
            vec!["0", "2.9"],
            vec!["0", "3"],
            vec!["0", "3.9"],
        ];
        let record_group_b = vec![vec!["0", "7.9"], vec!["0", "9"]];
        let record_group_c = vec![vec!["1", "4.1"], vec!["1", "6.9"]];

        let get_group_state = |record_group: Vec<Vec<&str>>| -> (bool, Option<u64>) {
            record_group
                .into_iter()
                .map(|raw_record| {
                    Record::new(
                        &ctx,
                        zip(headers.iter(), raw_record.iter())
                            .map(|(x, y)| (x.to_owned(), y.to_owned()))
                            .collect(),
                    )
                })
                .fold((true, None), |(is_same_group_id, group_id), x| {
                    if let Some(group_id) = group_id {
                        if is_same_group_id && x.group_id == group_id {
                            (true, Some(x.group_id))
                        } else {
                            (false, Some(x.group_id))
                        }
                    } else {
                        (true, Some(x.group_id))
                    }
                })
        };

        let (is_same_group_id_a, group_id_a) = get_group_state(record_group_a);
        let (is_same_group_id_b, group_id_b) = get_group_state(record_group_b);
        let (is_same_group_id_c, group_id_c) = get_group_state(record_group_c);
        assert_eq!(is_same_group_id_a, true);
        assert_eq!(is_same_group_id_b, true);
        assert_eq!(is_same_group_id_c, true);
        assert_ne!(group_id_a, group_id_b);
        assert_ne!(group_id_b, group_id_c);
        assert_ne!(group_id_c, group_id_a);
    }

    #[test]
    fn construct_collection() {
        let headers = vec!["userid", "time"];
        let ctx = make_a_ctx();
        let records = vec![
            // group_a
            vec!["0", "1.1"],
            vec!["0", "2.9"],
            vec!["0", "3"],
            vec!["0", "3.9"],
            // group_b
            vec!["0", "7.9"],
            vec!["0", "9"],
            // group_c
            vec!["1", "4.1"],
            vec!["1", "6.9"],
        ];
        let records: Vec<_> = records
            .into_iter()
            .map(|raw_record| {
                Record::new(
                    &ctx,
                    zip(headers.iter(), raw_record.into_iter())
                        .map(|(x, y)| (x.to_owned(), y))
                        .collect(),
                )
            })
            .collect();
        let collection = Collection::new(records.iter().collect());
        assert_eq!(collection.groups.len(), 3);
    }

    #[test]
    fn filter_collection() {
        let headers = vec!["userid", "time", "i"];
        let ctx = make_a_ctx();
        let records = vec![
            vec!["0", "1", "233"],
            vec!["0", "1", "23"],
            vec!["0", "1", "2333"],
            vec!["0", "1", "0"],
            vec!["0", "1", "-28"],
            vec!["0", "1", "233"],
            vec!["0", "1", "366"],
            vec!["0", "1", "243"],
        ];
        let records: Vec<_> = records
            .into_iter()
            .map(|raw_record| {
                Record::new(
                    &ctx,
                    zip(headers.iter(), raw_record.into_iter())
                        .map(|(x, y)| (x.to_owned(), y))
                        .collect(),
                )
            })
            .collect();
        let collection = Collection::new(records.iter().collect());
        let filter_cond = FilterCond {
            attr_name: "i".into(),
            val: Attr::Int(232),
            ord: Ordering::Greater,
        };
        let collection = collection.filter_records(filter_cond);

        assert_eq!(collection.groups.iter().next().unwrap().1.records.len(), 5);
    }

    #[test]
    fn set_operations_on_collections() {
        let headers = vec!["userid", "time", "i"];
        let ctx = make_a_ctx();
        let records = vec![
            vec!["0", "1", "233"],
            vec!["0", "1", "23"],
            vec!["0", "1", "2333"],
            vec!["0", "1", "0"],
            vec!["0", "1", "-28"],
            vec!["0", "1", "233"],
            vec!["0", "1", "366"],
            vec!["0", "1", "243"],
        ];
        let records: Vec<_> = records
            .into_iter()
            .map(|raw_record| {
                Record::new(
                    &ctx,
                    zip(headers.iter(), raw_record.into_iter())
                        .map(|(x, y)| (x.to_owned(), y))
                        .collect(),
                )
            })
            .collect();
        let whole_view = records.iter().collect();
        let end_with_3_view = records
            .iter()
            .filter(|record| {
                if let Some(Attr::Int(x)) = record.attrs.get("i") {
                    x % 10 == 3
                } else {
                    false
                }
            })
            .collect();
        let whole_view = Collection::new(whole_view);
        let end_with_3_view = Collection::new(end_with_3_view);
        let intersection = whole_view.intersection(&end_with_3_view);

        assert_eq!(
            intersection.groups.iter().next().unwrap().1.records.len(),
            5
        );

        let union = intersection.union(&end_with_3_view);
        assert_eq!(union.groups.iter().next().unwrap().1.records.len(), 5);

        let difference = union.difference(&end_with_3_view);
        assert_eq!(difference.groups.len(), 0);
    }

    #[test]
    fn fold_collections() {
        let headers = vec!["userid", "time", "i"];
        let ctx = make_a_ctx();
        let records = vec![
            vec!["0", "1", "233"],
            vec!["0", "1", "23"],
            vec!["0", "1", "2333"],
            vec!["0", "1", "0"],
            vec!["0", "1", "-28"],
            vec!["0", "1", "233"],
            vec!["0", "1", "366"],
            vec!["0", "1", "243"],
        ];
        let records: Vec<_> = records
            .into_iter()
            .map(|raw_record| {
                Record::new(
                    &ctx,
                    zip(headers.iter(), raw_record.into_iter())
                        .map(|(x, y)| (x.to_owned(), y))
                        .collect(),
                )
            })
            .collect();
        let view = records.iter().collect();
        let collection = Collection::new(view);

        let count_result = collection.count();
        assert_eq!(count_result.result.len(), 1);
        assert_eq!(count_result.result.iter().next().unwrap().1, &Attr::Int(8));

        let sum_result = collection.sum("i");
        assert_eq!(
            sum_result.result.iter().next().unwrap().1,
            &Attr::Float(3403f32)
        );

        let avg_result = collection.avg("i");
        assert_eq!(
            avg_result.result.iter().next().unwrap().1,
            &Attr::Float(425.375)
        );
    }

    #[test]
    #[should_panic(expected = "Error: header `x' is not found in context info")]
    fn unexpected_header() {
        let ctx = make_a_ctx();
        let headers = ["userid", "time", "x"];
        let record = vec!["0", "0", "0"];
        Record::new(&ctx, zip(headers.into_iter(), record.into_iter()).collect());
    }

    #[test]
    #[should_panic(expected = "Error: expect int when parsing attribute `i', which value is `true'")]
    fn invalid_attr_type() {
        let ctx = make_a_ctx();
        let headers = ["userid", "time", "i"];
        let record = vec!["0", "0", "true"];
        Record::new(&ctx, zip(headers.into_iter(), record.into_iter()).collect());
    }
}
