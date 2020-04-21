table! {
    refs (base_cid_id, referenced_base32_cidv1) {
        base_cid_id -> Int4,
        referenced_base32_cidv1 -> Text,
        name -> Text,
        size -> Int8,
    }
}

table! {
    resolved_cids (id) {
        id -> Int4,
        base32_cidv1 -> Text,
        type_id -> Int4,
        cumulative_size -> Int8,
        block_size -> Int8,
        links_size -> Int8,
        data_size -> Int8,
        num_links -> Int4,
        blocks -> Int4,
    }
}

table! {
    types (type_id) {
        type_id -> Int4,
        name -> Text,
    }
}

joinable!(refs -> resolved_cids (base_cid_id));
joinable!(resolved_cids -> types (type_id));

allow_tables_to_appear_in_same_query!(
    refs,
    resolved_cids,
    types,
);
