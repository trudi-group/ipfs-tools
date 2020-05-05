# ipfs-resolver-db

This is the database part of the ipfs-resolver project.
It produces a library that is used by a bunch of other components.

## Configuration

As with other packages, this also expects its configuration in a `.env` file.
The keys needed are:

- `DATABASE_URL` the PostgreSQL database URL, something like `postgres://localhost/ipfs`

## General Information

We use [diesel](https://crates.io/crates/diesel) as an ORM/query builder.
This is pretty nice because we get type-safe-at-compile-time queries.

The database is built from a series of migrations, which can be found in [migrations/](migrations).
It should be possible to easily set up the database using those migrations and the 
[diesel CLI](https://crates.io/crates/diesel_cli).