use crate::schema::*;

pub(crate) const TYPE_DIRECTORY: i32 = 1;
pub(crate) const TYPE_FILE: i32 = 2;

#[derive(Identifiable, Queryable, PartialEq, Debug)]
#[table_name = "types"]
#[primary_key(type_id)]
pub struct Type {
    pub type_id: i32,
    pub name: String,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[table_name = "resolved_cids"]
#[belongs_to(Type)]
#[primary_key(id)]
pub struct ResolvedCID {
    pub id: i32,
    pub base32_cidv1: String,
    pub type_id: i32,
    pub cumulative_size: i64,
    pub block_size: i64,
    pub links_size: i64,
    pub data_size: i64,
    pub num_links: i32,
    pub blocks: i32,
}

#[derive(Insertable)]
#[table_name = "resolved_cids"]
pub struct NewResolvedCID<'a> {
    pub base32_cidv1: &'a str,
    pub type_id: &'a i32,
    pub cumulative_size: &'a i64,
    pub block_size: &'a i64,
    pub links_size: &'a i64,
    pub data_size: &'a i64,
    pub num_links: &'a i32,
    pub blocks: &'a i32,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[table_name = "refs"]
#[belongs_to(ResolvedCID, foreign_key = "base_cid_id")]
#[primary_key(base_cid_id, referenced_base32_cidv1, name)]
pub struct Reference {
    pub base_cid_id: i32,
    pub referenced_base32_cidv1: String,
    pub name: String,
    pub size: i64,
}

#[derive(Insertable)]
#[table_name = "refs"]
pub struct NewReference<'a> {
    pub base_cid_id: &'a i32,
    pub referenced_base32_cidv1: &'a str,
    pub name: &'a str,
    pub size: &'a i64,
}
