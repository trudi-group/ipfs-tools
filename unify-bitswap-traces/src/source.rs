use crate::config::{Config, MonitorSourceConfig};
use crate::Result;
use failure::{Fail, ResultExt};
use flate2::read::GzDecoder;
use ipfs_resolver_common::wantlist;
use ipfs_resolver_common::wantlist::{EngineSimulation, JSONMessage};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

/// An Iterator that reads JSON messages from an ordered list of input files.
/// The input files are GZIP-decoded.
/// The order in which the input files are read is the order given by the expansion of the input
/// globs.
#[derive(Debug)]
struct MonitorSource {
    monitor_name: String,
    input_paths: Vec<PathBuf>,
    current_file: Option<BufReader<GzDecoder<File>>>,
    input_buffer: String,
}

impl MonitorSource {
    /// Expands input globs into paths (preserving the ordering of the globs) and constructs a
    /// `MonitorSource` from that.
    fn build_from_config(cfg: MonitorSourceConfig) -> Result<MonitorSource> {
        let paths = ipfs_resolver_common::expand_globs(&cfg.input_globs)
            .context("unable to expand globs")?;
        Ok(MonitorSource {
            monitor_name: cfg.monitor_name,
            input_paths: paths,
            current_file: None,
            input_buffer: String::new(),
        })
    }

    fn open_next_input_file(&mut self) -> Result<Option<BufReader<GzDecoder<File>>>> {
        if self.input_paths.is_empty() {
            return Ok(None);
        }

        // Popping off the front of this vector is not fast, but this is not performance critical...
        let p = self.input_paths.remove(0);
        let f = File::open(p).context("unable to open input file for reading")?;

        Ok(Some(BufReader::new(GzDecoder::new(f))))
    }
}

impl Iterator for MonitorSource {
    type Item = Result<JSONMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_file.is_none() {
                self.current_file = match self.open_next_input_file() {
                    Ok(f) => f,
                    Err(e) => break Some(Err(e.context("unable to open next input file").into())),
                }
            }

            match self.current_file.as_mut() {
                None => break None,
                Some(f) => {
                    self.input_buffer.clear();

                    let n = match f.read_line(&mut self.input_buffer) {
                        Ok(n) => n,
                        Err(e) => break Some(Err(e.context("unable to read input file").into())),
                    };

                    if n == 0 {
                        // EOF, try next file.
                        self.current_file = None;
                        continue;
                    }

                    let message: JSONMessage = match serde_json::from_str(&self.input_buffer) {
                        Ok(msg) => msg,
                        Err(e) => {
                            break Some(Err(e.context("unable to deserialize message").into()));
                        }
                    };
                    debug!(
                        "decoded message {:?} from monitor {}",
                        message, self.monitor_name
                    );

                    break Some(Ok(message));
                }
            }
        }
    }
}

/// A wrapper around a JSON message that implements ordering by timestamp.
#[derive(Clone, Debug)]
struct TimestampOrderedJSONMessage {
    msg: JSONMessage,
}

impl Ord for TimestampOrderedJSONMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.msg.timestamp.cmp(&other.msg.timestamp)
    }
}

impl PartialOrd for TimestampOrderedJSONMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.msg.timestamp.partial_cmp(&other.msg.timestamp)
    }
}

impl PartialEq for TimestampOrderedJSONMessage {
    fn eq(&self, other: &Self) -> bool {
        self.msg.timestamp.eq(&other.msg.timestamp)
    }
}

impl Eq for TimestampOrderedJSONMessage {}

/// An iterator adapter that sorts JSON messages by timestamp within a window.
/// The messages with lowest timestamp (i.e., the oldest messages) are emitted first.
///
/// The iterator adapts from an iterator of results to another iterator of results.
/// If the underlying iterator returns an error (i.e., reading from disk failed),
/// this iterator forwards that error.
/// Because some messages are possibly already buffered within this iterator, those messages
/// are lost.
/// We deem that acceptable, as reading from disk will fail the entire unification anyway.
struct WindowedJSONMessageSorter {
    input: MonitorSource,
    heap: BinaryHeap<std::cmp::Reverse<TimestampOrderedJSONMessage>>,
    window_size: usize,
}

impl WindowedJSONMessageSorter {
    /// Constructs a new windowed sorting iterator adapter for the given source and window size.
    fn new(src: MonitorSource, window_size: usize) -> WindowedJSONMessageSorter {
        WindowedJSONMessageSorter {
            input: src,
            heap: BinaryHeap::new(),
            window_size,
        }
    }
}

impl Iterator for WindowedJSONMessageSorter {
    type Item = Result<JSONMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        // Do we need to fill up the heap?
        while self.heap.len() < self.window_size {
            // Are there still messages to be read from disk?
            if let Some(msg) = self.input.next() {
                // Did we succeed in reading it?
                match msg {
                    Ok(msg) => {
                        // If yes: Push the next message onto the heap with correct ordering.
                        self.heap
                            .push(std::cmp::Reverse(TimestampOrderedJSONMessage { msg }))
                    }
                    Err(e) => {
                        // If no: Bubble that error through the iterators...
                        return Some(Err(e
                            .context(format!(
                                "unable to decode message from monitor {}",
                                self.input.monitor_name
                            ))
                            .into()));
                    }
                }
            } else {
                // If not: break from filling the heap, pop oldest message.
                break;
            }
        }

        // Pop oldest message off the heap.
        // If the heap is empty this will return None.
        self.heap.pop().map(|m| Ok(m.0.msg))
    }
}

/// This reads traces from multiple monitors, drives distinct bitswap engine simulations with them,
/// and produces an ordered iterator of the messages emitted.
pub(crate) struct MultiSourceIngester {
    source_names: Vec<String>,
    engine_states: Vec<EngineSimulation>,
    merged_sources: Box<dyn Iterator<Item = Result<(usize, JSONMessage)>>>,
    message_id: i64,
}

impl MultiSourceIngester {
    fn construct_sources(cfg: &Config) -> Result<Vec<MonitorSource>> {
        cfg.monitors
            .clone()
            .into_iter()
            .map(|c| MonitorSource::build_from_config(c))
            .collect::<std::result::Result<Vec<_>, _>>()
    }

    pub(crate) fn from_config(cfg: &Config) -> Result<MultiSourceIngester> {
        // Construct sources
        info!("constructing sources...");
        let sources = Self::construct_sources(cfg).context("unable to construct sources")?;
        debug!("constructed sources {:?}", sources);
        let source_names: Vec<_> = sources.iter().map(|s| s.monitor_name.clone()).collect();

        // Construct engine states
        let state = EngineSimulation::new(cfg.simulation_config.clone())
            .context("unable to set up engine simulation")?;
        let engine_states = std::iter::repeat(state)
            .take(sources.len())
            .collect::<Vec<_>>();

        // Make it so we can pop messages in order
        // Also sort them in windows, sheesh... (see journal on Nov 29th for an explanation).
        // This turns the sources into iterators that produce (source-index, message) tuples.
        // Pretty sick!
        let sources = sources
            .into_iter()
            .map(|s| WindowedJSONMessageSorter::new(s, cfg.message_sorting_window_size))
            .enumerate()
            .map(|(k, s)| s.map(move |msg| msg.map(|msg| (k, msg))))
            .collect::<Vec<_>>();
        // And now we k-way merge them by timestamp.
        let merged_sources = itertools::kmerge_by(
            sources.into_iter(),
            |a: &Result<(usize, JSONMessage)>, b: &Result<(usize, JSONMessage)>| {
                // This is pretty hacky, but:
                // We see if any of the two is an error, and if yes sort it towards the front of
                // the queue.
                // This is dirty because of many things, but it should be okay because we will
                // cancel the entire unification if there's an error reading from disk anyway, so
                // it should be fine.
                match &a {
                    Ok((_, msg1)) => {
                        match &b {
                            Ok((_, msg2)) => {
                                // TODO what if they are equal?
                                msg1.timestamp < msg2.timestamp
                            }
                            Err(_) => false,
                        }
                    }
                    Err(_) => true,
                }
            },
        );

        Ok(MultiSourceIngester {
            source_names,
            engine_states,
            merged_sources: Box::new(merged_sources),
            message_id: 0,
        })
    }

    pub(crate) fn source_names(&self) -> Vec<String> {
        self.source_names.clone()
    }

    pub(crate) fn into_engine_states(self) -> Vec<EngineSimulation> {
        self.engine_states
    }

    // The ID of the last message produced by the iterator.
    pub(crate) fn last_message_id(&self) -> i64 {
        self.message_id
    }
}

impl Iterator for MultiSourceIngester {
    type Item = Result<MultiSourceIngestResult>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.merged_sources.next() {
            Some(elem) => {
                match elem {
                    Ok((monitor_id, msg)) => {
                        debug!("popped message for monitor {}: {:?}", monitor_id, msg);
                        self.message_id += 1;

                        // Update engine state for that monitor
                        match self.engine_states[monitor_id].ingest(&msg, self.message_id) {
                            Ok(ingest_result) => {
                                debug!("got ingest result {:?}", ingest_result);
                                Some(Ok(MultiSourceIngestResult {
                                    monitor_id,
                                    timestamp: msg.timestamp,
                                    peer_id: msg.peer,
                                    simulation_result: ingest_result,
                                }))
                            }
                            Err(e) => {
                                debug!("unable to ingest: {:?}", e);
                                Some(Err(e
                                    .context(
                                        "unable to update engine simulation state with new message",
                                    )
                                    .into()))
                            }
                        }
                    }
                    Err(e) => {
                        return Some(Err(e.context("unable to get message from source").into()));
                    }
                }
            }
            None => None,
        }
    }
}

/// The item produced by the `MultiSourceIngester`.
pub(crate) struct MultiSourceIngestResult {
    pub(crate) monitor_id: usize,
    pub(crate) timestamp: chrono::DateTime<chrono::Utc>,
    pub(crate) peer_id: String,
    pub(crate) simulation_result: wantlist::IngestResult,
}
