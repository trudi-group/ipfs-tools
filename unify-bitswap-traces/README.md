# Bitswap Trace Unification

## How does this work

1. There are multiple monitors
2. One `MonitorSource` per monitor, which reads JSON objects from a list of gzipped JSON files
3. Those are sorted within a window (because some nondeterminism, maybe in Go, maybe in SSHfs).
    See `WindowedJSONMessageSorter`.
4. All those iterators are then taken and a bitswap engine is simulated for each of them.
    See `MultiSourceIngester`.
    This produces synthetic entries (ignored here), connection event entries (ignored here), and marks duplicates.
    These are duplicates as determined by one monitor.
    See the [ipfs-json-to-csv tool](../ipfs-json-to-csv) which does this, basically.
5. The engine simulations are driven by the message streams in timestamp order.
    See `MultiSourceIngester`.
6. Whatever comes out of the engine simulations is then emitted, again in timestamp order (but they are ordered because
    the messages were ingested in timestamp order)
7. This unified stream of bitswap engine simulation results is checked for global duplicates between the monitors.
    These duplicates are marked based on whether the entry has been emitted by any monitor within some window.
    Global duplicates should always be a superset of per-monitor duplicates if the window sizes are the same.
    The most recent candidate seen by any monitor will be selected as a duplicate.
8. Additionally, entries are matched between monitors, which is similar to finding duplicates, but usually operates on
    a much smaller window.
    This has a few configurables, see below.
    Synthetic entries are not matched.
9. In the end, all (non-connection-event) entries from the bitswap engine simulations are emitted.
    Each entry is annotated with
    - Origin monitor
    - Per-monitor duplicate status (and properties)
    - Global duplicate status (and properties)
    - Global matching status (and properties)
    Synthetic entries are not matched between monitors.

We ignore connection events (and don't emit them) because they are unreliable, and I haven't found a use for unified connection events yet.
We ignore synthetic entries _for_matching_, but we do emit them.


## Configuration

### Global configuration

This configures some general things, like output file names.
We also sort each iterator of JSON messages (coming from a monitor, see below) within a small window.
In our setup, the entries are _mostly_ sorted, but apparently there's some nondeterminism in there somewhere...
The `wantlist_output_file_pattern` should contain an `$id$` placeholder, which will be replaced with the numeric ID of the first message in that output file.
Output files will be rotated every 100k (or so) entries, because otherwise R dies trying to read one massive CSV file...

```
message_sorting_window_size: 1000
wantlist_output_file_pattern: "csv/wl-$id$.csv.gz"
ledger_count_output_file: "csv/ledgers.csv.gz"
```

### `monitors` Configuration

The `monitors` block configures which traces to use as inputs.
Each monitor has a name and a glob which expands to the input files to read.
The globs will be expanded in order, and the results of their expansion will be used to simulate ledgers and ultimately produce output entries.
The files should be read in chronological order, i.e., the entries should be ordered by timestamp.
The files will be read one after another on-demand, and iterators of their entries will be merged by timestamp.

### `simulation_config`

This configures the engine simulation being run on each monitor.
The engine simulation produces synthetic entries and marks per-monitor duplicates.
See [../common/src/wantlist.rs](../common/src/wantlist.rs) for explanations of the properties.

```
simulation_config:
  allow_empty_full_wantlist: false
  allow_empty_connection_event: false
  insert_full_wantlist_synth_cancels: true
  insert_disconnect_synth_cancels: true
  reconnect_duplicate_duration_secs: 5
  sliding_window_lengths: [1,9,11,29,31,601,3601,604801]
```

### `matching_config`

This configures the matching of entries between the multiple monitors' entries.
The properties are documented in [src/config.rs](src/config.rs).

```
matching_config:
  inter_monitor_matching_window_milliseconds: 5000
  global_duplicate_window_seconds: 31
  match_newest_first: false
  allow_multiple_match: false
  match_exact_entry_type: false
```

Long story short:
- There are two windows:
  - One for marking global duplicates. These are entries which are equal but did not necessarily originate on the same
      monitor. This is usually a large window, 31 seconds in our case, because IPFS re-broadcasts entries every 30
      seconds in some cases.
  - One for marking matched entries between monitors. This is smaller and there are a few rules for matching entries.
- The matching can be configured based on
  - Whether to prefer matching to the oldest or newest (= most recent) entry in the window
  - Whether to match the exact entry type (i.e., `WANT_HAVE_SEND_DONT_HAVE`) or just match any request entry type with
      any other request entry type (and `CANCEL`s with `CANCEL`s)
  - Whether to allow matching an entry multiple times

This illustration might help to understand the matching configuration:
```
//                          (oldest)  - - - >  (newest)
// Window Monitor 1:        A  B   C   D   A   D
//                                                  Now assume an entry A comes in via Monitor 2
//                                                  The question is:
// - match with which?      ^              ^       <- A Monitor 2
//                                         ^ matched with this
//                                                  Now assume another entry A comes in via Monitor 2
//                                                  The next question is:
// - match multiple times?                 ^             <- A Monitor 2
```

## Configuration used for the paper submission to NSDI

This is the configuration used for [this paper](https://arxiv.org/abs/2104.09202).
Version 1 of the paper uses a different week, but all other parameters are identical.

```yaml
# This is a config file for the unify-bitswap-traces tool.
monitors:
  - monitor_name: "de1"
    input_globs:
      - "../../../archive/wantlists-de1/wantlist.json.2021-04-30*.gz"
      - "../../../archive/wantlists-de1/wantlist.json.2021-05-01*.gz"
      - "../../../archive/wantlists-de1/wantlist.json.2021-05-02*.gz"
      - "../../../archive/wantlists-de1/wantlist.json.2021-05-03*.gz"
      - "../../../archive/wantlists-de1/wantlist.json.2021-05-04*.gz"
      - "../../../archive/wantlists-de1/wantlist.json.2021-05-05*.gz"
      - "../../../archive/wantlists-de1/wantlist.json.2021-05-06*.gz"
  - monitor_name: "us1"
    input_globs:
      - "../../../archive/wantlists-us1/wantlist.json.2021-04-30*.gz"
      - "../../../archive/wantlists-us1/wantlist.json.2021-05-01*.gz"
      - "../../../archive/wantlists-us1/wantlist.json.2021-05-02*.gz"
      - "../../../archive/wantlists-us1/wantlist.json.2021-05-03*.gz"
      - "../../../archive/wantlists-us1/wantlist.json.2021-05-04*.gz"
      - "../../../archive/wantlists-us1/wantlist.json.2021-05-05*.gz"
      - "../../../archive/wantlists-us1/wantlist.json.2021-05-06*.gz"
message_sorting_window_size: 1000
wantlist_output_file_pattern: "csv/wl-$id$.csv.gz"
ledger_count_output_file: "csv/ledgers.csv.gz"
matching_config:
  inter_monitor_matching_window_milliseconds: 5000
  global_duplicate_window_seconds: 31
  match_newest_first: false
  allow_multiple_match: false
  match_exact_entry_type: false
simulation_config:
  allow_empty_full_wantlist: false
  allow_empty_connection_event: false
  insert_full_wantlist_synth_cancels: true
  insert_disconnect_synth_cancels: true
  reconnect_duplicate_duration_secs: 5
  sliding_window_lengths: [1,9,11,29,31,601,3601,604801]
```