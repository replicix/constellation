//! The drop rules a marker applies to a replica's member copies, in the model
//! (`constellation_model::backup::marker_keeps`) and in the product
//! (`Meta::apply_segment_rows`'s `member_rules_tx`), over the same cases. The
//! model is the cheap proof that a rule never drops a row a later claim
//! needs; a rule the product has and the model lacks (the claim without a
//! tail settling the older epochs, review round 2 of ADR-57) would go
//! unnoticed without this.

use constellation_meta::{BackupTx, LogRecord, MemberTx, Meta, RowCommit, TouchSet};
use constellation_model::backup::{marker_keeps, Class};

fn product_class(c: Class) -> RowCommit {
    match c {
        Class::Lone => RowCommit::LONE,
        Class::RootGroup => RowCommit::ROOT_GROUP,
        Class::Archive => RowCommit::ARCHIVE,
    }
}

fn row(id: u8, class: Class) -> MemberTx {
    MemberTx {
        tx: BackupTx {
            first: id as u64,
            last: id as u64,
            records: vec![LogRecord::Create {
                parent: 1,
                name: format!("f{id}"),
                ino: 100 + id as u64,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 1,
            }],
            origin: (0, 0),
        },
        class: product_class(class),
        rid: None,
        op: None,
        lock_tag: Default::default(),
    }
}

/// The marker of epoch 3 the product would write for what the model says it
/// follows: a claim over no tail says `{e - 1, 0}`; a named tail says the
/// contiguous hold its rows reach.
fn product_marker(e: u8, follows: Option<(u8, &[u8])>) -> Vec<LogRecord> {
    let (prev, through) = match follows {
        None => (e - 1, 0),
        Some((prev, rows)) => (prev, rows.iter().copied().max().unwrap_or(0)),
    };
    vec![LogRecord::TailFollowsThrough {
        prev_epoch: prev as u64,
        through: through as u64,
    }]
}

#[test]
fn the_models_marker_rules_match_the_products() {
    let e = 3u8;
    type Follows = Option<(u8, Vec<u8>)>;
    let cases: Vec<(&str, Follows)> = vec![
        ("no tail named", None),
        ("a named tail that ships nothing", Some((2, vec![]))),
        ("a named tail of the previous epoch", Some((2, vec![1, 2]))),
        ("a named tail of an older epoch", Some((1, vec![1, 2]))),
        ("an older epoch named with nothing", Some((1, vec![]))),
    ];
    for (what, follows) in cases {
        let follows_ref = follows.as_ref().map(|(p, r)| (*p, &r[..]));
        let meta = Meta::open_in_memory().unwrap();
        let mut all: Vec<(u8, u8, Class)> = Vec::new();
        for me in [1u8, 2] {
            let rows: Vec<MemberTx> = [1u8, 2, 3]
                .iter()
                .flat_map(|id| {
                    [Class::RootGroup, Class::Archive]
                        .into_iter()
                        // (One row per id and class: ids differ by class.)
                        .map(move |c| (id + if c == Class::Archive { 10 } else { 0 }, c))
                })
                .map(|(id, c)| {
                    all.push((me, id, c));
                    row(id, c)
                })
                .collect();
            meta.member_copy_put_rows(me as u64, 1, &rows).unwrap();
        }
        meta.apply_segment_rows(
            1,
            e as u64,
            0,
            &[],
            &[],
            &product_marker(e, follows_ref),
            &TouchSet::default(),
        )
        .unwrap();
        let mut product: Vec<(u8, u8)> = Vec::new();
        for me in [1u8, 2] {
            if let Some((_, rows)) = meta.member_copy_rows(me as u64).unwrap() {
                product.extend(rows.iter().map(|r| (me, r.tx.first as u8)));
            }
        }
        product.sort();
        // (The model's ids of a tail it re-ships are the plain 1..=3, which
        // the product's through reaches; the archive twins 11..=13 are not
        // in it.)
        let mut model: Vec<(u8, u8)> = all
            .iter()
            .filter(|(me, id, c)| marker_keeps(*me, *id, *c, e, follows_ref, false))
            .map(|(me, id, _)| (*me, *id))
            .collect();
        model.sort();
        assert_eq!(
            model, product,
            "{what}: the model's survivors against the product's"
        );
    }
}
