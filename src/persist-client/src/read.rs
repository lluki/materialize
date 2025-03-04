// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Read capabilities and handles

use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures::Stream;
use mz_ore::task::RuntimeExt;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::runtime::Handle;
use tracing::{debug_span, info, instrument, trace, trace_span, warn, Instrument};
use uuid::Uuid;

use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::Blob;
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};

use crate::error::InvalidUsage;
use crate::r#impl::encoding::SerdeSnapshotSplit;
use crate::r#impl::machine::{retry_external, Machine};
use crate::r#impl::metrics::Metrics;
use crate::r#impl::paths::PartialBlobKey;
use crate::r#impl::state::Since;
use crate::{PersistConfig, ShardId};

/// An opaque identifier for a reader of a persist durable TVC (aka shard).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ReaderId(pub(crate) [u8; 16]);

impl std::fmt::Display for ReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "r{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for ReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReaderId({})", Uuid::from_bytes(self.0))
    }
}

impl ReaderId {
    pub(crate) fn new() -> Self {
        ReaderId(*Uuid::new_v4().as_bytes())
    }
}

/// A token representing one split of a "snapshot" (the contents of a shard
/// as of some frontier).
///
/// This may be exchanged (including over the network). It is tradeable via
/// [ReadHandle::snapshot_iter] for a [SnapshotIter], which can be used to
/// receive the relevant data.
///
/// See [ReadHandle::snapshot] for details.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Timestamp + Codec64",
    deserialize = "T: Timestamp + Codec64"
))]
#[serde(into = "SerdeSnapshotSplit", from = "SerdeSnapshotSplit")]
pub struct SnapshotSplit<T> {
    pub(crate) reader_id: ReaderId,
    pub(crate) shard_id: ShardId,
    pub(crate) as_of: Antichain<T>,
    pub(crate) batches: Vec<(PartialBlobKey, Description<T>)>,
}

/// An iterator over one split of a "snapshot" (the contents of a shard as of
/// some frontier).
///
/// See [ReadHandle::snapshot] for details.
#[derive(Debug)]
pub struct SnapshotIter<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64,
{
    handle: ReadHandle<K, V, T, D>,
    as_of: Antichain<T>,
    batches: Vec<(PartialBlobKey, Description<T>)>,

    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> SnapshotIter<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    fn new(handle: ReadHandle<K, V, T, D>, split: SnapshotSplit<T>) -> Self {
        debug_assert_eq!(handle.reader_id, split.reader_id);
        SnapshotIter {
            handle,
            as_of: split.as_of,
            batches: split.batches,
            _phantom: PhantomData,
        }
    }

    /// The frontier at which we're outputting the contents of the shard.
    pub fn as_of(&self) -> &Antichain<T> {
        &self.as_of
    }

    /// Attempt to pull out the next values of this iterator.
    ///
    /// All times emitted will have been [advanced by] the [Self::as_of]
    /// frontier.
    ///
    /// [advanced by]: Lattice::advance_by
    ///
    /// The returned updates are not consolidated. In the presence of
    /// compaction, consolidation can take an unbounded amount of memory so it's
    /// not safe for persist to consolidate in the general case. Persist users
    /// that know they are dealing with a small amount of data are free to
    /// consolidate this themselves. See
    /// [differential_dataflow::consolidation::consolidate_updates].
    ///
    /// An None value is returned if this iterator is exhausted.
    #[instrument(level = "debug", name = "snap::next", skip_all, fields(shard = %self.handle.machine.shard_id()))]
    pub async fn next(&mut self) -> Option<Vec<((Result<K, String>, Result<V, String>), T, D)>> {
        trace!("SnapshotIter::next");

        loop {
            let (key, desc) = match self.batches.pop() {
                Some(x) => {
                    // Periodically heartbeat so the batches we're iterating
                    // don't get garbage collected.
                    self.handle.maybe_heartbeat_reader().await;
                    x
                }
                // All done! Drop our seqno capability to unblock garbage
                // collection of blobs.
                None => {
                    trace!("SnapshotIter::expire");
                    self.handle
                        .machine
                        .expire_reader(&self.handle.reader_id)
                        .await;
                    self.handle.explicitly_expired = true;
                    return None;
                }
            };

            let mut updates = Vec::new();
            fetch_batch_part(
                &self.handle.machine.shard_id(),
                self.handle.blob.as_ref(),
                &self.handle.metrics,
                &key,
                &desc,
                |k, v, mut t, d| {
                    // This would get covered by a listen started at the same as_of.
                    if self.as_of.less_than(&t) {
                        return;
                    }
                    t.advance_by(self.as_of.borrow());
                    let k = self.handle.metrics.codecs.key.decode(|| K::decode(k));
                    let v = self.handle.metrics.codecs.val.decode(|| V::decode(v));
                    let d = D::decode(d);
                    updates.push(((k, v), t, d));
                },
            )
            .await;
            if updates.is_empty() {
                // We might have filtered everything.
                continue;
            }
            return Some(updates);
        }
    }

    /// Politely expires this iterator, releasing its lease.
    ///
    /// There is a best-effort impl in Drop to expire a iterator that wasn't
    /// explictly expired with this method. When possible, explicit expiry is
    /// still preferred because the Drop one is best effort and is dependant on
    /// a tokio [Handle] being available in the TLC at the time of drop (which
    /// is a bit subtle). Also, explicit expiry allows for control over when it
    /// happens.
    pub async fn expire(self) {
        self.handle.expire().await
    }
}

impl<K, V, T, D> SnapshotIter<K, V, T, D>
where
    K: Debug + Codec + Ord,
    V: Debug + Codec + Ord,
    T: Timestamp + Lattice + Codec64 + Ord,
    D: Semigroup + Codec64 + Ord,
{
    /// Test helper to read all data in the snapshot and return it sorted.
    #[cfg(test)]
    #[track_caller]
    pub async fn read_all(&mut self) -> Vec<((Result<K, String>, Result<V, String>), T, D)> {
        let mut ret = Vec::new();
        while let Some(mut next) = self.next().await {
            ret.append(&mut next)
        }
        ret.sort();
        ret
    }
}

/// Data and progress events of a shard subscription.
///
/// TODO: Unify this with [timely::dataflow::operators::to_stream::Event] or
/// [timely::dataflow::operators::capture::event::Event].
#[derive(Debug, PartialEq)]
pub enum ListenEvent<K, V, T, D> {
    /// Progress of the shard.
    Progress(Antichain<T>),
    /// Data of the shard.
    Updates(Vec<((Result<K, String>, Result<V, String>), T, D)>),
}

/// An ongoing subscription of updates to a shard.
#[derive(Debug)]
pub struct Listen<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64,
{
    handle: ReadHandle<K, V, T, D>,
    as_of: Antichain<T>,

    since: Antichain<T>,
    frontier: Antichain<T>,
}

impl<K, V, T, D> Listen<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    async fn new(handle: ReadHandle<K, V, T, D>, as_of: Antichain<T>) -> Self {
        let mut ret = Listen {
            handle,
            since: as_of.clone(),
            frontier: as_of.clone(),
            as_of,
        };

        // This listen only needs to distinguish things after its frontier
        // (initially as_of although the frontier is inclusive and the as_of
        // isn't). Be a good citizen and downgrade early.
        ret.handle.downgrade_since(ret.since.clone()).await;
        ret
    }

    /// Convert listener into futures::Stream
    pub fn into_stream(mut self) -> impl Stream<Item = ListenEvent<K, V, T, D>> {
        async_stream::stream!({
            loop {
                for msg in self.next().await {
                    yield msg;
                }
            }
        })
    }

    /// Attempt to pull out the next values of this subscription.
    ///
    /// The updates received in [ListenEvent::Updates] should be assumed to be in arbitrary order
    /// and not necessarily consolidated. However, the timestamp of each individual update will be
    /// greater than or equal to the last received [ListenEvent::Progress] frontier (or this
    /// [Listen]'s initial `as_of` frontier if no progress event has been emitted yet) and less
    /// than the next [ListenEvent::Progress] frontier.
    ///
    /// If you have a use for consolidated listen output, given that snapshots can't be
    /// consolidated, come talk to us!
    #[instrument(level = "debug", name = "listen::next", skip_all, fields(shard = %self.handle.machine.shard_id()))]
    pub async fn next(&mut self) -> Vec<ListenEvent<K, V, T, D>> {
        trace!("Listen::next");

        // We might also want to call maybe_heartbeat_reader in the
        // `next_listen_batch` loop so that we can heartbeat even when batches
        // aren't incoming. At the moment, the ownership would be weird and we
        // should always have batches incoming regularly, so maybe it isn't
        // actually necessary.
        let batch = self.handle.machine.next_listen_batch(&self.frontier).await;

        // A lot of things across mz have to line up to hold the following
        // invariant and violations only show up as subtle correctness errors,
        // so explictly validate it here. Better to panic and roll back a
        // release than be incorrect (also potentially corrupting a sink).
        //
        // Note that the since check is intentionally less_than, not less_equal.
        // If a batch's since is X, that means we can no longer distinguish X
        // (beyond self.frontier) from X-1 (not beyond self.frontier) to keep
        // former and filter out the latter.
        assert!(
            PartialOrder::less_than(batch.desc.since(), &self.frontier)
                // Special case when the frontier == the as_of (i.e. the first
                // time this is called on a new Listen). Because as_of is
                // _exclusive_, we don't need to be able to distinguish X from
                // X-1.
                || (self.frontier == self.as_of
                    && PartialOrder::less_equal(batch.desc.since(), &self.frontier)),
            "Listen on {} received a batch {:?} advanced past the listen frontier {:?}",
            self.handle.machine.shard_id(),
            batch.desc,
            self.frontier
        );

        let mut updates = Vec::new();
        for key in batch.keys.iter() {
            fetch_batch_part(
                &self.handle.machine.shard_id(),
                self.handle.blob.as_ref(),
                &self.handle.metrics,
                key,
                &batch.desc,
                |k, v, t, d| {
                    // This would get covered by a snapshot started at the same as_of.
                    if !self.as_of.less_than(&t) {
                        return;
                    }
                    // Because of compaction, the next batch we get might also
                    // contain updates we've already emitted. For example, we
                    // emitted `[1, 2)` and then compaction combined that batch
                    // with a `[2, 3)` batch into a new `[1, 3)` batch. If this
                    // happens, we just need to filter out anything < the
                    // frontier. This frontier was the upper of the last batch
                    // (and thus exclusive) so for the == case, we still emit.
                    if !self.frontier.less_equal(&t) {
                        return;
                    }
                    let k = self.handle.metrics.codecs.key.decode(|| K::decode(k));
                    let v = self.handle.metrics.codecs.val.decode(|| V::decode(v));
                    let d = D::decode(d);
                    updates.push(((k, v), t, d));
                },
            )
            .await;
        }
        let mut ret = Vec::with_capacity(2);
        if !updates.is_empty() {
            ret.push(ListenEvent::Updates(updates));
        }
        ret.push(ListenEvent::Progress(batch.desc.upper().clone()));
        self.frontier = batch.desc.upper().clone();

        // We have a new frontier, so this is an opportunity to downgrade our
        // since capability. Go through `maybe_heartbeat` so we can rate limit
        // this along with our heartbeats.
        //
        // HACK! Everything would be simpler if we could downgrade since to the
        // new frontier, but we can't. The next call needs to be able to
        // distinguish between the times T at the frontier (to emit updates with
        // these times) and T-1 (to filter them). Advancing the since to
        // frontier would erase the ability to distinguish between them. Ideally
        // we'd use what is conceptually "frontier - 1" (the greatest elements
        // that are still strictly less than frontier), but the trait bounds on
        // T don't give us a way to compute that directly. Instead, we sniff out
        // any elements in lower that are strictly less_than frontier to compute
        // a new since. For totally ordered times (currently always the case in
        // mz) lower will always have a single element and it will be less_than
        // upper, but the following logic is (hopefully) correct for partially
        // order times as well. We could also abuse the fact that every time we
        // actually emit is guaranteed by definition to be less_than upper to be
        // a bit more prompt, but this would involve a lot more temporary
        // antichains and it's unclear if that's worth it.
        for l in batch.desc.lower().elements().iter() {
            let lower_than_upper = batch.desc.upper().elements().iter().any(|u| l.less_than(u));
            if lower_than_upper {
                self.since.join_assign(&Antichain::from_elem(l.clone()));
            }
        }
        self.handle.maybe_downgrade_since(&self.since).await;

        ret
    }

    /// Politely expires this listen, releasing its lease.
    ///
    /// There is a best-effort impl in Drop to expire a listen that wasn't
    /// explictly expired with this method. When possible, explicit expiry is
    /// still preferred because the Drop one is best effort and is dependant on
    /// a tokio [Handle] being available in the TLC at the time of drop (which
    /// is a bit subtle). Also, explicit expiry allows for control over when it
    /// happens.
    pub async fn expire(self) {
        self.handle.expire().await
    }

    /// Test helper to read from the listener until the given frontier is
    /// reached. Because compaction can arbitrarily combine batches, we only
    /// return the final progress info.
    #[cfg(test)]
    #[track_caller]
    pub async fn read_until(
        &mut self,
        ts: &T,
    ) -> (
        Vec<((Result<K, String>, Result<V, String>), T, D)>,
        Antichain<T>,
    ) {
        let mut updates = Vec::new();
        let mut frontier = Antichain::from_elem(T::minimum());
        while self.frontier.less_than(ts) {
            for event in self.next().await {
                match event {
                    ListenEvent::Updates(mut x) => updates.append(&mut x),
                    ListenEvent::Progress(x) => frontier = x,
                }
            }
        }
        // Unlike most tests, intentionally don't consolidate updates here
        // because Listen replays them at the original fidelity.
        (updates, frontier)
    }
}

/// A "capability" granting the ability to read the state of some shard at times
/// greater or equal to `self.since()`.
///
/// Production users should call [Self::expire] before dropping a ReadHandle so
/// that it can expire its leases. If/when rust gets AsyncDrop, this will be
/// done automatically.
///
/// All async methods on ReadHandle retry for as long as they are able, but the
/// returned [std::future::Future]s implement "cancel on drop" semantics. This
/// means that callers can add a timeout using [tokio::time::timeout] or
/// [tokio::time::timeout_at].
///
/// ```rust,no_run
/// # let mut read: mz_persist_client::read::ReadHandle<String, String, u64, i64> = unimplemented!();
/// # let timeout: std::time::Duration = unimplemented!();
/// # let new_since: timely::progress::Antichain<u64> = unimplemented!();
/// # async {
/// tokio::time::timeout(timeout, read.downgrade_since(new_since)).await
/// # };
/// ```
#[derive(Debug)]
pub struct ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64,
{
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) reader_id: ReaderId,
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) blob: Arc<dyn Blob + Send + Sync>,

    pub(crate) since: Antichain<T>,
    pub(crate) last_heartbeat: Instant,
    pub(crate) explicitly_expired: bool,
}

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// This handle's `since` frontier.
    ///
    /// This will always be greater or equal to the shard-global `since`.
    pub fn since(&self) -> &Antichain<T> {
        &self.since
    }

    /// Forwards the since frontier of this handle, giving up the ability to
    /// read at times not greater or equal to `new_since`.
    ///
    /// This may trigger (asynchronous) compaction and consolidation in the
    /// system. A `new_since` of the empty antichain "finishes" this shard,
    /// promising that no more data will ever be read by this handle.
    ///
    /// This also acts as a heartbeat for the reader lease (including if called
    /// with `new_since` equal to `self.since()`, making the call a no-op).
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn downgrade_since(&mut self, new_since: Antichain<T>) {
        trace!("ReadHandle::downgrade_since new_since={:?}", new_since);
        let (_seqno, current_reader_since) = self
            .machine
            .downgrade_since(&self.reader_id, &new_since, (self.cfg.now)())
            .await;
        self.since = current_reader_since.0;
        // A heartbeat is just any downgrade_since traffic, so update the
        // internal rate limiter here to play nicely with `maybe_heartbeat`.
        self.last_heartbeat = Instant::now();
    }

    /// Returns an ongoing subscription of updates to a shard.
    ///
    /// The stream includes all data at times greater than `as_of`. Combined
    /// with [Self::snapshot] it will produce exactly correct results: the
    /// snapshot is the TVCs contents at `as_of` and all subsequent updates
    /// occur at exactly their indicated time. The recipient should only
    /// downgrade their read capability when they are certain they have all data
    /// through the frontier they would downgrade to.
    ///
    /// This takes ownership of the ReadHandle so the Listen can use it to
    /// [Self::downgrade_since] as it progresses. If you need to keep this
    /// handle, then [Self::clone] it before calling listen.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn listen(self, as_of: Antichain<T>) -> Result<Listen<K, V, T, D>, Since<T>> {
        trace!("ReadHandle::listen as_of={:?}", as_of);

        let () = self.machine.verify_listen(&as_of).await?;
        Ok(Listen::new(self, as_of).await)
    }

    /// Returns a snapshot of the contents of the shard TVC at `as_of`.
    ///
    /// This command returns the contents of this shard as of `as_of` once they
    /// are known. This may "block" (in an async-friendly way) if `as_of` is
    /// greater or equal to the current `upper` of the shard. The recipient
    /// should only downgrade their read capability when they are certain they
    /// have all data through the frontier they would downgrade to.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    ///
    /// This is a convenience method for constructing the snapshot and
    /// immediately consuming it from a single place. If you need to parallelize
    /// snapshot iteration (potentially from multiple machines), see
    /// [Self::snapshot_splits] and [Self::snapshot_iter].
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn snapshot(
        &self,
        as_of: Antichain<T>,
    ) -> Result<SnapshotIter<K, V, T, D>, Since<T>> {
        let mut splits = self
            .snapshot_splits(as_of, NonZeroUsize::new(1).unwrap())
            .await?;
        assert_eq!(splits.len(), 1);
        let split = splits.pop().unwrap();
        let iter = self
            .snapshot_iter(split)
            .await
            .expect("internal error: snapshot shard didn't match machine shard");
        Ok(iter)
    }

    /// Returns a snapshot of the contents of the shard TVC at `as_of`.
    ///
    /// This command returns the contents of this shard as of `as_of` once they
    /// are known. This may "block" (in an async-friendly way) if `as_of` is
    /// greater or equal to the current `upper` of the shard. The recipient
    /// should only downgrade their read capability when they are certain they
    /// have all data through the frontier they would downgrade to.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    ///
    /// This snapshot may be split into a number of splits, each of which may be
    /// exchanged (including over the network) to load balance the processing of
    /// this snapshot. These splits are usable by anyone with access to the
    /// shard's [crate::PersistLocation]. The `len()` of the returned `Vec` is
    /// `num_splits`. If a 1:1 mapping between splits and (e.g. dataflow
    /// workers) is used, then the work of replaying the snapshot will be
    /// roughly balanced.
    ///
    /// This method exists to allow users to parallelize snapshot iteration. If
    /// you want to immediately consume the snapshot from a single place, you
    /// likely want the [Self::snapshot] helper.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn snapshot_splits(
        &self,
        as_of: Antichain<T>,
        num_splits: NonZeroUsize,
    ) -> Result<Vec<SnapshotSplit<T>>, Since<T>> {
        trace!(
            "ReadHandle::snapshot as_of={:?} num_splits={:?}",
            as_of,
            num_splits
        );
        // Hack: Keep this method `&self` instead of `&mut self` by cloning the
        // cached copy of the state, updating it, and throwing it away
        // afterward.
        let mut machine = self.machine.clone();
        let batches = machine.snapshot(&as_of).await?;
        let batches = batches.into_iter().flat_map(|b| {
            let desc = b.desc.clone();
            b.keys.into_iter().map(move |k| (k, desc.clone()))
        });
        let mut splits = Vec::new();
        for _ in 0..num_splits.get() {
            // Register this split as a new reader so that it gets its own
            // seqno capability.
            let split_reader_id = ReaderId::new();
            let _read_cap = machine
                .clone_reader(&split_reader_id, (self.cfg.now)())
                .await;
            // We don't want the frontier capability, so explictly toss it.
            let _ = machine
                .downgrade_since(&split_reader_id, &Antichain::new(), (self.cfg.now)())
                .await;
            splits.push(SnapshotSplit {
                reader_id: split_reader_id,
                shard_id: self.machine.shard_id(),
                as_of: as_of.clone(),
                batches: Vec::new(),
            })
        }
        for (idx, (batch_key, desc)) in batches.into_iter().enumerate() {
            splits[idx % num_splits.get()]
                .batches
                .push((batch_key, desc.clone()));
        }
        return Ok(splits);
    }

    /// Trade in an exchange-able [SnapshotSplit] for an iterator over the data
    /// it represents.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn snapshot_iter(
        &self,
        split: SnapshotSplit<T>,
    ) -> Result<SnapshotIter<K, V, T, D>, InvalidUsage<T>> {
        trace!("ReadHandle::snapshot split={:?}", split);
        if split.shard_id != self.machine.shard_id() {
            return Err(InvalidUsage::SnapshotNotFromThisShard {
                snapshot_shard: split.shard_id,
                handle_shard: self.machine.shard_id(),
            });
        }
        let mut machine = self.machine.clone();
        machine.fetch_and_update_state().await;
        let handle = ReadHandle {
            cfg: self.cfg.clone(),
            metrics: Arc::clone(&self.metrics),
            reader_id: split.reader_id.clone(),
            machine,
            blob: Arc::clone(&self.blob),
            since: self.since.clone(),
            // This isn't quite right since we did the heartbeat before sending
            // it over the wire, but probably good enough for now? Maybe we
            // should roundtrip the timestamp through SnapshotSplit instead?
            last_heartbeat: Instant::now(),
            explicitly_expired: false,
        };
        let iter = SnapshotIter::new(handle, split);
        Ok(iter)
    }

    /// Returns an independent [ReadHandle] with a new [ReaderId] but the same
    /// `since`.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn clone(&self) -> Self {
        trace!("ReadHandle::clone");
        let new_reader_id = ReaderId::new();
        let mut machine = self.machine.clone();
        let read_cap = machine.clone_reader(&new_reader_id, (self.cfg.now)()).await;
        let new_reader = ReadHandle {
            cfg: self.cfg.clone(),
            metrics: Arc::clone(&self.metrics),
            reader_id: new_reader_id,
            machine,
            blob: Arc::clone(&self.blob),
            since: read_cap.since,
            last_heartbeat: Instant::now(),
            explicitly_expired: false,
        };
        new_reader
    }

    /// A rate-limited version of [Self::downgrade_since].
    ///
    /// This is an internally rate limited helper, designed to allow users to
    /// call it as frequently as they like. Call this [Self::downgrade_since],
    /// or [Self::maybe_heartbeat_reader] on some interval that is "frequent"
    /// compared to PersistConfig::FAKE_READ_LEASE_DURATION.
    ///
    /// This is communicating actual progress information, so is given
    /// preferential treatment compared to [Self::maybe_heartbeat_reader].
    pub async fn maybe_downgrade_since(&mut self, new_since: &Antichain<T>) {
        // NB: min_elapsed is intentionally smaller than the one in
        // maybe_heartbeat_reader (this is the preferential treatment mentioned
        // above).
        let min_elapsed = PersistConfig::FAKE_READ_LEASE_DURATION / 4;
        let elapsed_since_last_heartbeat = self.last_heartbeat.elapsed();
        if elapsed_since_last_heartbeat >= min_elapsed {
            self.downgrade_since(new_since.clone()).await;
        }
    }

    /// Heartbeats the read lease if necessary.
    ///
    /// This is an internally rate limited helper, designed to allow users to
    /// call it as frequently as they like. Call this [Self::downgrade_since],
    /// or [Self::maybe_downgrade_since] on some interval that is "frequent"
    /// compared to PersistConfig::FAKE_READ_LEASE_DURATION.
    pub async fn maybe_heartbeat_reader(&mut self) {
        let min_elapsed = PersistConfig::FAKE_READ_LEASE_DURATION / 2;
        let elapsed_since_last_heartbeat = self.last_heartbeat.elapsed();
        if elapsed_since_last_heartbeat >= min_elapsed {
            self.machine
                .heartbeat_reader(&self.reader_id, (self.cfg.now)())
                .await;
            self.last_heartbeat = Instant::now();
        }
    }

    /// Politely expires this reader, releasing its lease.
    ///
    /// There is a best-effort impl in Drop to expire a reader that wasn't
    /// explictly expired with this method. When possible, explicit expiry is
    /// still preferred because the Drop one is best effort and is dependant on
    /// a tokio [Handle] being available in the TLC at the time of drop (which
    /// is a bit subtle). Also, explicit expiry allows for control over when it
    /// happens.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn expire(mut self) {
        trace!("ReadHandle::expire");
        self.machine.expire_reader(&self.reader_id).await;
        self.explicitly_expired = true;
    }

    /// Test helper for an [Self::snapshot] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_snapshot(&self, as_of: T) -> SnapshotIter<K, V, T, D> {
        self.snapshot(Antichain::from_elem(as_of))
            .await
            .expect("cannot serve requested as_of")
    }

    /// Test helper for a [Self::listen] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_listen(self, as_of: T) -> Listen<K, V, T, D> {
        self.listen(Antichain::from_elem(as_of))
            .await
            .expect("cannot serve requested as_of")
    }
}

impl<K, V, T, D> Drop for ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in this auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64,
{
    fn drop(&mut self) {
        if self.explicitly_expired {
            return;
        }
        let handle = match Handle::try_current() {
            Ok(x) => x,
            Err(_) => {
                warn!("ReadHandle {} dropped without being explicitly expired, falling back to lease timeout", self.reader_id);
                return;
            }
        };
        let mut machine = self.machine.clone();
        let reader_id = self.reader_id.clone();
        // Spawn a best-effort task to expire this read handle. It's fine if
        // this doesn't run to completion, we'd just have to wait out the lease
        // before the shard-global since is unblocked.
        //
        // Intentionally create the span outside the task to set the parent.
        let expire_span = debug_span!("drop::expire");
        let _ = handle.spawn_named(
            || format!("ReadHandle::expire ({})", self.reader_id),
            async move {
                trace!("ReadHandle::expire");
                machine.expire_reader(&reader_id).await;
            }
            .instrument(expire_span),
        );
    }
}

pub(crate) async fn fetch_batch_part<T, UpdateFn>(
    shard_id: &ShardId,
    blob: &(dyn Blob + Send + Sync),
    metrics: &Metrics,
    key: &PartialBlobKey,
    registered_desc: &Description<T>,
    mut update_fn: UpdateFn,
) where
    T: Timestamp + Lattice + Codec64,
    UpdateFn: FnMut(&[u8], &[u8], T, [u8; 8]),
{
    let mut retry = metrics
        .retries
        .fetch_batch_part
        .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
    let get_span = debug_span!("fetch_batch::get");
    let value = loop {
        let value = retry_external(&metrics.retries.external.fetch_batch_get, || async {
            blob.get(&key.complete(shard_id)).await
        })
        .instrument(get_span.clone())
        .await;
        match value {
            Some(x) => break x,
            // If the underlying impl of blob isn't linearizable, then we
            // might get a key reference that that blob isn't returning yet.
            // Keep trying, it'll show up.
            None => {
                // This is quite unexpected given that our initial blobs _are_
                // linearizable, so always log at info.
                info!(
                    "unexpected missing blob, trying again in {:?}: {}",
                    retry.next_sleep(),
                    key
                );
                retry = retry.sleep().await;
            }
        };
    };
    drop(get_span);

    trace_span!("fetch_batch::decode").in_scope(|| {
        let batch = metrics
            .codecs
            .batch
            .decode(|| BlobTraceBatchPart::decode(&value))
            .map_err(|err| anyhow!("couldn't decode batch at key {}: {}", key, err))
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");

        // Drop the encoded representation as soon as we can to reclaim memory.
        drop(value);

        // There are two types of batches in persist:
        // - Batches written by a persist user (either directly or indirectly
        //   via BatchBuilder). These always have a since of the minimum
        //   timestamp and may be registered in persist state with a tighter set
        //   of bounds than are inline in the batch (truncation). To read one of
        //   these batches, all data physically in the batch but outside of the
        //   truncated bounds must be ignored. Not every user batch is
        //   truncated.
        // - Batches written by compaction. These always have an inline desc
        //   that exactly matches the one they are registered with. The since
        //   can be anything.
        let inline_desc = decode_inline_desc(&batch.desc);
        let needs_truncation = inline_desc.lower() != registered_desc.lower()
            || inline_desc.upper() != registered_desc.upper();
        if needs_truncation {
            assert!(
                PartialOrder::less_equal(inline_desc.lower(), registered_desc.lower()),
                "key={} inline={:?} registered={:?}",
                key,
                inline_desc,
                registered_desc
            );
            assert!(
                PartialOrder::less_equal(registered_desc.upper(), inline_desc.upper()),
                "key={} inline={:?} registered={:?}",
                key,
                inline_desc,
                registered_desc
            );
            // As mentioned above, batches that needs truncation will always have a
            // since of the minimum timestamp. Technically we could truncate any
            // batch where the since is less_than the output_desc's lower, but we're
            // strict here so we don't get any surprises.
            assert_eq!(
                inline_desc.since(),
                &Antichain::from_elem(T::minimum()),
                "key={} inline={:?} registered={:?}",
                key,
                inline_desc,
                registered_desc
            );
        } else {
            assert_eq!(
                &inline_desc, registered_desc,
                "key={} inline={:?} registered={:?}",
                key, inline_desc, registered_desc
            );
        }

        for chunk in batch.updates {
            for ((k, v), t, d) in chunk.iter() {
                let t = T::decode(t);

                // This filtering is really subtle, see the comment above for
                // what's going on here.
                if needs_truncation {
                    if !registered_desc.lower().less_equal(&t) {
                        continue;
                    }
                    if registered_desc.upper().less_equal(&t) {
                        continue;
                    }
                }

                update_fn(k, v, t, d);
            }
        }
    })
}

// TODO: This goes away the desc on BlobTraceBatchPart becomes a Description<T>,
// which should be a straightforward refactor but it touches a decent bit.
fn decode_inline_desc<T: Timestamp + Codec64>(desc: &Description<u64>) -> Description<T> {
    fn decode_antichain<T: Timestamp + Codec64>(x: &Antichain<u64>) -> Antichain<T> {
        Antichain::from(
            x.elements()
                .iter()
                .map(|x| T::decode(x.to_le_bytes()))
                .collect::<Vec<_>>(),
        )
    }
    Description::new(
        decode_antichain(desc.lower()),
        decode_antichain(desc.upper()),
        decode_antichain(desc.since()),
    )
}

// WIP the way skip_consensus_fetch_optimization works is pretty fundamentally
// incompatible with maintaining a lease
#[cfg(test)]
#[cfg(WIP)]
mod tests {
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist::location::Consensus;
    use mz_persist::mem::{MemBlob, MemBlobConfig, MemConsensus};
    use mz_persist::unreliable::{UnreliableConsensus, UnreliableHandle};
    use timely::ExchangeData;

    use crate::r#impl::metrics::Metrics;
    use crate::tests::all_ok;
    use crate::{PersistClient, PersistConfig};

    use super::*;

    // Verifies performance optimizations where a SnapshotIter/Listener doesn't
    // fetch the latest Consensus state if the one it currently has can serve
    // the next request.
    #[tokio::test]
    async fn skip_consensus_fetch_optimization() {
        mz_ore::test::init_logging();
        let data = vec![
            (("0".to_owned(), "zero".to_owned()), 0, 1),
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let blob = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let consensus = Arc::new(MemConsensus::default());
        let unreliable = UnreliableHandle::default();
        unreliable.totally_available();
        let consensus = Arc::new(UnreliableConsensus::new(consensus, unreliable.clone()))
            as Arc<dyn Consensus + Send + Sync>;
        let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
        let (mut write, read) = PersistClient::new(
            PersistConfig::new(SYSTEM_TIME.clone()),
            blob,
            consensus,
            metrics,
        )
        .await
        .expect("client construction failed")
        .expect_open::<String, String, u64, i64>(ShardId::new())
        .await;

        write.expect_compare_and_append(&data[0..1], 0, 1).await;
        write.expect_compare_and_append(&data[1..2], 1, 2).await;
        write.expect_compare_and_append(&data[2..3], 2, 3).await;

        let mut snapshot = read.expect_snapshot(2).await;
        let mut listen = read.expect_listen(0).await;

        // Manually advance the listener's machine so that it has the latest
        // state by fetching the first events from next. This is awkward but
        // only necessary because we're about to do some weird things with
        // unreliable.
        let listen_actual = listen.next().await;
        let expected_events = vec![ListenEvent::Progress(Antichain::from_elem(1))];
        assert_eq!(listen_actual, expected_events);

        // At this point, the snapshot and listen's state should have all the
        // writes. Test this by making consensus completely unavailable.
        unreliable.totally_unavailable();
        assert_eq!(snapshot.read_all().await, all_ok(&data, 2));
        assert_eq!(
            listen.read_until(&3).await,
            (all_ok(&data[1..], 1), Antichain::from_elem(3))
        );
    }

    #[test]
    fn snapshot_split_exchange_data() {
        // The whole point of SnapshotSplit is that it can be exchanged between
        // timely workers, including over the network. Enforce then that it
        // implements ExchangeData.
        fn is_exchange_data<T: ExchangeData>() {}
        is_exchange_data::<SnapshotSplit<u64>>();
        is_exchange_data::<SnapshotSplit<i64>>();
    }
}
