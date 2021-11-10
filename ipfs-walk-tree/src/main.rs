#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;

use ipfs_resolver_db::model;
use ipfs_resolver_db::{canonicalize_cid_from_str_to_cidv1, db};

use cursive::traits::*;
use cursive::views::{Dialog, TextView};
use cursive::Cursive;
use cursive_async_view::{AsyncProgressState, AsyncProgressView};
use cursive_table_view::{TableView, TableViewItem};
use diesel::PgConnection;
use failure::_core::cmp::Ordering;
use failure::{err_msg, ResultExt};
use ipfs_resolver_common::{logging, Result};
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use cursive::CursiveExt;

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    logging::set_up_logging(false)?;

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        return Err(err_msg("missing CID argument"));
    }
    let c = canonicalize_cid_from_str_to_cidv1(&args[1]).context("invalid CID")?;
    let cid_bytes = c.to_bytes();

    info!("reading .env...");
    dotenv::dotenv().ok();

    info!("connecting to DB...");
    let conn = ipfs_resolver_db::establish_connection()?;

    info!("finding occurrences...");
    let occurrences = db::find_unixfs_links_by_cid(&conn, &cid_bytes)?;

    if occurrences.len() == 0 {
        println!("not referenced by anything, sorry :(");
        return Ok(());
    }
    info!("found {} occurrences", occurrences.len());

    let mut siv = Cursive::default();
    siv.add_global_callback('q', |s| s.quit());
    siv.add_global_callback(
        cursive::event::Event::Key(cursive::event::Key::Backspace),
        go_up_one_level,
    );

    siv.set_user_data(UserData {
        navigation: Vec::new(),
        conn: Arc::new(Mutex::new(conn)),
        loading: Arc::new(Mutex::new(false)),
    });

    add_entry_point_layer(&mut siv, &occurrences)?;

    siv.run();

    Ok(())
}

struct UserData {
    navigation: Vec<NavigationElement>,
    conn: Arc<Mutex<PgConnection>>,
    loading: Arc<Mutex<bool>>,
}

#[derive(Clone, Debug)]
enum NavigationElement {
    EntryPointTable { cid: Vec<u8>, table_name: String },
    UnixFSLinksTable { block_id: i32, table_name: String },
}

fn go_up_one_level(siv: &mut Cursive) {
    let nav = siv.user_data::<UserData>().unwrap();
    assert!(!nav.navigation.is_empty());
    if *nav.loading.lock().unwrap() {
        return;
    }
    if nav.navigation.len() == 1 {
        return;
    }

    let layer = nav.navigation.pop();
    debug!("popped navigation layer {:?}", layer);
    siv.pop_layer();
}

fn add_unixfs_links_table(siv: &mut Cursive, children: &Vec<model::UnixFSLink>) -> Result<()> {
    let data = siv.user_data::<UserData>().unwrap();
    let conn = data.conn.clone();
    let loading = data.loading.clone();
    {
        let mut l = loading.lock().unwrap();
        *l = true;
    }
    let nav = &mut data.navigation;
    let block_id = children[0].parent_block_id;
    let table_name = format!("links_{}", block_id);
    let num_children = children.len();

    nav.push(NavigationElement::UnixFSLinksTable {
        block_id,
        table_name: table_name.clone(),
    });

    let (tx, rx) = mpsc::channel();
    let thread_children = children.clone();

    thread::spawn(move || {
        let conn = conn.lock().unwrap();
        for c in thread_children.iter() {
            debug!(
                "getting data for link ({},f{})",
                c.parent_block_id,
                hex::encode(&c.referenced_cidv1)
            );

            let b = db::find_block_by_cid(&conn, &c.referenced_cidv1).unwrap();
            let bb = if let Some(block) = b {
                let resolves = db::get_successful_resolves_for_block(&conn, block.id).unwrap();
                let links = db::get_unixfs_links_for_block(&conn, block.id).unwrap();
                Some(ExistingBlock {
                    links,
                    block,
                    successful_resolves: resolves,
                })
            } else {
                None
            };

            let parents = db::find_unixfs_links_by_cid(&conn, &c.referenced_cidv1).unwrap();

            tx.send(UnixFSLink {
                link: c.clone(),
                block: bb,
                parents,
            })
            .unwrap();
        }

        {
            let mut l = loading.lock().unwrap();
            *l = false;
        }
    });

    let mut links = Vec::new();

    let async_view = AsyncProgressView::new(siv, move || {
        match rx.try_recv() {
            Ok(msg) => {
                links.push(msg);

                AsyncProgressState::Pending(links.len() as f32 / num_children as f32)
            }

            Err(TryRecvError::Empty) => {
                AsyncProgressState::Pending(links.len() as f32 / num_children as f32)
            }

            // Channel closed, finished.
            Err(TryRecvError::Disconnected) => {
                let mut table = TableView::<UnixFSLink, DirectoryListingColumn>::new()
                    .column(
                        DirectoryListingColumn::ChildSize,
                        DirectoryListingColumn::ChildSize.as_str(),
                        |c| c.width(10),
                    )
                    .column(
                        DirectoryListingColumn::ChildName,
                        DirectoryListingColumn::ChildName.as_str(),
                        |c| c,
                    )
                    .column(
                        DirectoryListingColumn::NumUncles,
                        DirectoryListingColumn::NumUncles.as_str(),
                        |c| c.width(11),
                    )
                    .column(
                        DirectoryListingColumn::ChildBlockID,
                        DirectoryListingColumn::ChildBlockID.as_str(),
                        |c| c.width(12),
                    )
                    .column(
                        DirectoryListingColumn::NumChildren,
                        DirectoryListingColumn::NumChildren.as_str(),
                        |c| c.width(10),
                    )
                    .column(
                        DirectoryListingColumn::ChildLastSuccessfulResolve,
                        DirectoryListingColumn::ChildLastSuccessfulResolve.as_str(),
                        |c| c.width(18),
                    );

                table.set_items(links.clone());

                let inner_table_name = table_name.to_string();
                table.set_on_submit(move |siv: &mut Cursive, _row: usize, index: usize| {
                    let item = siv
                        .call_on_name(
                            &inner_table_name,
                            move |table: &mut TableView<UnixFSLink, DirectoryListingColumn>| {
                                let item = table.borrow_item(index).unwrap();
                                item.clone()
                            },
                        )
                        .unwrap();

                    let title = format!(
                        "Child {} of UnixFS block {}",
                        hex::encode(&item.link.referenced_cidv1),
                        item.link.parent_block_id
                    );

                    if let Some(block) = item.block.clone() {
                        let mut diag = Dialog::around(TextView::new(format!(
                            "Parent Block: {}\n\
                                    Link CID: f{}\n\
                                    Linked item's ID: {}\n\
                                    Linked item's children: {}\n\
                                    Total number of parents of linked content: {}\n",
                            item.link.parent_block_id,
                            hex::encode(&item.link.referenced_cidv1),
                            block.block.id,
                            block.links.len(),
                            item.parents.len()
                        )))
                        .title(title);

                        if block.links.len() > 0 {
                            diag = diag.button("Look at the children!", move |s| {
                                s.pop_layer();
                                add_unixfs_links_table(s, &block.links).unwrap();
                            });
                        }
                        diag = diag
                            .button("Find other parents of this!", move |s| {
                                s.pop_layer();
                                add_entry_point_layer(s, &item.parents).unwrap();
                            })
                            .button("Back to the table!", move |s| {
                                s.pop_layer();
                            });

                        siv.add_layer(diag);
                    } else {
                        siv.add_layer(
                            Dialog::around(TextView::new(format!(
                                "Parent Block: {}\n\
                                    Link CID: f{}\n\
                                    Total number of parents of linked content: {}\n",
                                item.link.parent_block_id,
                                hex::encode(&item.link.referenced_cidv1),
                                item.parents.len()
                            )))
                            .title(title)
                            .button("Find other parents (=uncles) of this!", move |s| {
                                s.pop_layer();
                                add_entry_point_layer(s, &item.parents).unwrap();
                            })
                            .button("Back to the table!", move |s| {
                                s.pop_layer();
                            }),
                        );
                    }
                });

                AsyncProgressState::Available(
                    Dialog::around(table.with_name(table_name.clone()).min_size((200, 40)))
                        .title(format!("UnixFS links of block {}", block_id)),
                )
            }
        }
    })
    .with_width(80)
    .with_height(10);

    siv.add_layer(Dialog::around(async_view));

    Ok(())
}

fn add_entry_point_layer(siv: &mut Cursive, occurrences: &Vec<model::UnixFSLink>) -> Result<()> {
    let data = siv.user_data::<UserData>().unwrap();
    let conn = data.conn.clone();
    let loading = data.loading.clone();
    {
        let mut l = loading.lock().unwrap();
        *l = true;
    }
    let nav = &mut data.navigation;
    let cid_string = hex::encode(&occurrences[0].referenced_cidv1);
    let cid_bytes = occurrences[0].referenced_cidv1.clone();
    let table_name = format!("refs_{}", cid_string);
    let num_occurrences = occurrences.len();

    nav.push(NavigationElement::EntryPointTable {
        cid: cid_bytes,
        table_name: table_name.clone(),
    });

    let (tx, rx) = mpsc::channel();
    let thread_occurrences = occurrences.clone();

    thread::spawn(move || {
        let conn = conn.lock().unwrap();
        for occ in thread_occurrences.iter() {
            debug!("getting data for block {}..", occ.parent_block_id);

            let b = db::get_block(&conn, occ.parent_block_id).unwrap();
            let resolves =
                db::get_successful_resolves_for_block(&conn, occ.parent_block_id).unwrap();
            let links = db::get_unixfs_links_for_block(&conn, occ.parent_block_id).unwrap();
            let child_index = links
                .iter()
                .position(|l| l.referenced_cidv1.eq(&occ.referenced_cidv1))
                .unwrap();
            let grandparents = db::find_unixfs_links_by_cid(&conn, &b.cidv1).unwrap();

            tx.send(UnixFSEntryPoint {
                child_index,
                block: ExistingBlock {
                    links,
                    block: b,
                    successful_resolves: resolves,
                },
                parents: grandparents,
            })
            .unwrap();
        }

        {
            let mut l = loading.lock().unwrap();
            *l = false;
        }
    });

    let mut entry_points = Vec::new();

    let async_view = AsyncProgressView::new(siv, move || {
        match rx.try_recv() {
            Ok(msg) => {
                entry_points.push(msg);

                AsyncProgressState::Pending(entry_points.len() as f32 / num_occurrences as f32)
            }

            Err(TryRecvError::Empty) => {
                AsyncProgressState::Pending(entry_points.len() as f32 / num_occurrences as f32)
            }

            // Channel closed, finished.
            Err(TryRecvError::Disconnected) => {
                let mut table = TableView::<UnixFSEntryPoint, UnixFSEntryPointColumn>::new()
                    .column(
                        UnixFSEntryPointColumn::ParentBlockID,
                        UnixFSEntryPointColumn::ParentBlockID.as_str(),
                        |c| c.width(12),
                    )
                    .column(
                        UnixFSEntryPointColumn::ParentLastSuccessfulResolve,
                        UnixFSEntryPointColumn::ParentLastSuccessfulResolve.as_str(),
                        |c| c.width(18),
                    )
                    .column(
                        UnixFSEntryPointColumn::ParentNumLinks,
                        UnixFSEntryPointColumn::ParentNumLinks.as_str(),
                        |c| c.width(13),
                    )
                    .column(
                        UnixFSEntryPointColumn::NumGrandparents,
                        UnixFSEntryPointColumn::NumGrandparents.as_str(),
                        |c| c.width(17),
                    )
                    .column(
                        UnixFSEntryPointColumn::ChildName,
                        UnixFSEntryPointColumn::ChildName.as_str(),
                        |c| c,
                    )
                    .column(
                        UnixFSEntryPointColumn::ChildSize,
                        UnixFSEntryPointColumn::ChildSize.as_str(),
                        |c| c.width(14),
                    );

                table.set_items(entry_points.clone());

                let inner_table_name = table_name.clone();
                table.set_on_submit(move |siv: &mut Cursive, _row: usize, index: usize| {
                    let item = siv
                        .call_on_name(
                            &inner_table_name,
                            move |table: &mut TableView<UnixFSEntryPoint, UnixFSEntryPointColumn>| {
                                let item = table.borrow_item(index).unwrap();
                                item.clone()
                            },
                        )
                        .unwrap();

                    let title=format!("Entry point of {} at parent block {}",
                                      hex::encode(&item.block.links[item.child_index].referenced_cidv1),
                                      item.block.block.id);

                    if item.parents.is_empty() {
                        siv.add_layer(
                            Dialog::around(TextView::new(
                                format!(
                                    "Parent: {}\nParent CID: f{}\nName: {}\nCID: f{}\nSiblings: {}",
                                    item.block.block.id,
                                    hex::encode(&item.block.block.cidv1),
                                    item.block.links[item.child_index].name.to_string(),
                                    hex::encode(&item.block.links[item.child_index].referenced_cidv1),
                                    item.block.links.len()
                                )))
                                .title(title)
                                .button("Look at the siblings!", move |s| {
                                    s.pop_layer();
                                    add_unixfs_links_table(s, &item.block.links).unwrap();
                                })
                                .button("Back to the table!",move |s| {s.pop_layer();}),
                        );
                    } else {
                        let siblings = item.block.links.clone();
                        let parents = item.parents;
                        siv.add_layer(
                            Dialog::around(TextView::new(
                                format!(
                                    "Parent: {}\nParent CID: f{}\nName: {}\nCID: f{}\nSiblings: {}\nGrandparents: {}",
                                    item.block.block.id,
                                    hex::encode(&item.block.block.cidv1),
                                    siblings[item.child_index].name.to_string(),
                                    hex::encode(&siblings[item.child_index].referenced_cidv1),
                                    siblings.len(),
                                    parents.len()
                                )))
                                .title(title)
                                .button("Look at the siblings!", move |s| {
                                    s.pop_layer();
                                    add_unixfs_links_table(s, &siblings).unwrap();
                                })
                                .button("Investigate the grandparents!", move |s| {
                                    s.pop_layer();
                                    add_entry_point_layer(s, &parents).unwrap();
                                })
                                .button("Back to the table!",move |s| {s.pop_layer();}),
                        );
                    }
                });

                AsyncProgressState::Available(
                    Dialog::around(table.with_name(table_name.clone()).min_size((200, 40)))
                        .title(format!("References to {}", cid_string)),
                )
            }
        }
    });

    siv.add_layer(Dialog::around(async_view.with_width(80).with_height(10)));

    Ok(())
}

#[derive(Clone, Debug)]
struct ExistingBlock {
    block: model::Block,
    successful_resolves: Vec<model::SuccessfulResolve>,
    links: Vec<model::UnixFSLink>,
}

#[derive(Clone, Debug)]
struct UnixFSEntryPoint {
    block: ExistingBlock,
    child_index: usize,
    parents: Vec<model::UnixFSLink>,
}

#[derive(Clone, Debug)]
struct UnixFSLink {
    link: model::UnixFSLink,
    block: Option<ExistingBlock>,
    parents: Vec<model::UnixFSLink>,
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum UnixFSEntryPointColumn {
    ParentBlockID,
    ParentCID,
    ParentNumLinks,
    ParentLastSuccessfulResolve,
    ChildName,
    ChildSize,
    NumGrandparents,
}

impl UnixFSEntryPointColumn {
    fn as_str(&self) -> &str {
        match *self {
            UnixFSEntryPointColumn::ParentBlockID => "Parent ID",
            UnixFSEntryPointColumn::ParentCID => "CID",
            UnixFSEntryPointColumn::ParentNumLinks => "#Siblings",
            UnixFSEntryPointColumn::ParentLastSuccessfulResolve => "Last Resolved",
            UnixFSEntryPointColumn::ChildName => "Entry Name",
            UnixFSEntryPointColumn::ChildSize => "Entry Size",
            UnixFSEntryPointColumn::NumGrandparents => "#Grandparents",
        }
    }
}

impl TableViewItem<UnixFSEntryPointColumn> for UnixFSEntryPoint {
    fn to_column(&self, column: UnixFSEntryPointColumn) -> String {
        match column {
            UnixFSEntryPointColumn::ParentBlockID => format!("{}", self.block.block.id),
            UnixFSEntryPointColumn::ParentCID => {
                format!("f{}", hex::encode(&self.block.block.cidv1))
            }
            UnixFSEntryPointColumn::ParentNumLinks => format!("{}", self.block.links.len()),
            UnixFSEntryPointColumn::ParentLastSuccessfulResolve => {
                if self.block.successful_resolves.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "{}",
                        self.block.successful_resolves[0]
                            .ts
                            .format("%Y-%m-%d %H:%M")
                    )
                }
            }
            UnixFSEntryPointColumn::ChildName => {
                self.block.links[self.child_index].name.to_string()
            }
            UnixFSEntryPointColumn::ChildSize => {
                format!("{}", self.block.links[self.child_index].size)
            }
            UnixFSEntryPointColumn::NumGrandparents => format!("{}", self.parents.len()),
        }
    }

    fn cmp(&self, other: &Self, column: UnixFSEntryPointColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            UnixFSEntryPointColumn::ParentBlockID => self.block.block.id.cmp(&other.block.block.id),
            UnixFSEntryPointColumn::ParentCID => {
                self.block.block.cidv1.cmp(&other.block.block.cidv1)
            }
            UnixFSEntryPointColumn::ParentNumLinks => {
                self.block.links.len().cmp(&other.block.links.len())
            }
            UnixFSEntryPointColumn::ParentLastSuccessfulResolve => {
                if self.block.successful_resolves.is_empty() {
                    if other.block.successful_resolves.is_empty() {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                } else {
                    if other.block.successful_resolves.is_empty() {
                        Ordering::Greater
                    } else {
                        self.block.successful_resolves[0]
                            .ts
                            .cmp(&other.block.successful_resolves[0].ts)
                    }
                }
            }
            UnixFSEntryPointColumn::ChildName => self.block.links[self.child_index]
                .name
                .cmp(&other.block.links[other.child_index].name),
            UnixFSEntryPointColumn::ChildSize => self.block.links[self.child_index]
                .size
                .cmp(&other.block.links[other.child_index].size),
            UnixFSEntryPointColumn::NumGrandparents => self.parents.len().cmp(&other.parents.len()),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum DirectoryListingColumn {
    ChildName,
    ChildCID,
    ChildSize,
    ChildBlockID,
    ChildLastSuccessfulResolve,
    NumUncles,
    NumChildren,
}

impl DirectoryListingColumn {
    fn as_str(&self) -> &str {
        match *self {
            DirectoryListingColumn::ChildName => "Name",
            DirectoryListingColumn::ChildCID => "CID",
            DirectoryListingColumn::ChildSize => "Size",
            DirectoryListingColumn::ChildBlockID => "Child ID",
            DirectoryListingColumn::ChildLastSuccessfulResolve => "Child Last Resolved",
            DirectoryListingColumn::NumUncles => "#Uncles",
            DirectoryListingColumn::NumChildren => "#Children",
        }
    }
}

impl TableViewItem<DirectoryListingColumn> for UnixFSLink {
    fn to_column(&self, column: DirectoryListingColumn) -> String {
        match column {
            DirectoryListingColumn::ChildName => self.link.name.to_string(),
            DirectoryListingColumn::ChildCID => {
                format!("f{}", hex::encode(&self.link.referenced_cidv1))
            }
            DirectoryListingColumn::ChildSize => format!("{}", self.link.size),
            DirectoryListingColumn::ChildBlockID => {
                if let Some(block) = &self.block {
                    format!("{}", block.block.id)
                } else {
                    "".to_string()
                }
            }
            DirectoryListingColumn::ChildLastSuccessfulResolve => {
                if let Some(block) = &self.block {
                    if block.successful_resolves.is_empty() {
                        "".to_string()
                    } else {
                        format!(
                            "{}",
                            block.successful_resolves[0].ts.format("%Y-%m-%d %H:%M")
                        )
                    }
                } else {
                    "".to_string()
                }
            }
            DirectoryListingColumn::NumUncles => format!("{}", self.parents.len()),
            DirectoryListingColumn::NumChildren => {
                if let Some(block) = &self.block {
                    format!("{}", block.links.len())
                } else {
                    "".to_string()
                }
            }
        }
    }

    fn cmp(&self, other: &Self, column: DirectoryListingColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            DirectoryListingColumn::ChildName => self.link.name.cmp(&other.link.name),
            DirectoryListingColumn::ChildCID => {
                self.link.referenced_cidv1.cmp(&other.link.referenced_cidv1)
            }
            DirectoryListingColumn::ChildSize => self.link.size.cmp(&other.link.size),
            DirectoryListingColumn::ChildBlockID => {
                if let Some(block) = &self.block {
                    if let Some(other_block) = &other.block {
                        block.block.id.cmp(&other_block.block.id)
                    } else {
                        Ordering::Greater
                    }
                } else {
                    if other.block.is_some() {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                }
            }
            DirectoryListingColumn::ChildLastSuccessfulResolve => {
                if let Some(block) = &self.block {
                    if let Some(other_block) = &other.block {
                        if block.successful_resolves.is_empty() {
                            if other_block.successful_resolves.is_empty() {
                                Ordering::Equal
                            } else {
                                Ordering::Less
                            }
                        } else {
                            if other_block.successful_resolves.is_empty() {
                                Ordering::Greater
                            } else {
                                block.successful_resolves[0]
                                    .ts
                                    .cmp(&other_block.successful_resolves[0].ts)
                            }
                        }
                    } else {
                        Ordering::Greater
                    }
                } else {
                    if other.block.is_some() {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                }
            }
            DirectoryListingColumn::NumUncles => self.parents.len().cmp(&other.parents.len()),
            DirectoryListingColumn::NumChildren => {
                if let Some(block) = &self.block {
                    if let Some(other_block) = &other.block {
                        block.links.len().cmp(&other_block.links.len())
                    } else {
                        Ordering::Greater
                    }
                } else {
                    if other.block.is_some() {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                }
            }
        }
    }
}
