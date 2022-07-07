// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by differential dataflow.

use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::logging::DifferentialEvent;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use timely::communication::Allocate;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::logging::WorkerIdentifier;

use crate::compute_state::ComputeState;
use crate::logging::persist::persist_sink;
use mz_expr::{permutation_for_arrangement, MirScalarExpr};
use mz_repr::{Datum, DatumVec, Diff, Row, Timestamp};
use mz_timely_util::activator::RcActivator;
use mz_timely_util::replay::MzReplay;

use crate::logging::{ConsolidateBuffer, DifferentialLog, LogVariant};
use crate::typedefs::{KeysValsHandle, RowSpine};

/// Constructs the logging dataflow for differential logs.
///
/// Params
/// * `worker`: The Timely worker hosting the log analysis dataflow.
/// * `config`: Logging configuration
/// * `linked`: The source to read log events from.
/// * `activator`: A handle to acknowledge activations.
///
/// Returns a map from log variant to a tuple of a trace handle and a permutation to reconstruct
/// the original rows.
pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &mz_compute_client::logging::LoggingConfig,
    compute_state: &mut ComputeState,
    linked: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, DifferentialEvent)>>,
    activator: RcActivator,
) -> HashMap<LogVariant, (KeysValsHandle, Rc<dyn Any>)> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns / 1_000_000) as Timestamp;

    let traces = worker.dataflow_named("Dataflow: differential logging", move |scope| {
        let (logs, token) = Some(linked).mz_replay(
            scope,
            "differential logs",
            Duration::from_nanos(config.granularity_ns as u64),
            activator,
        );

        let mut demux =
            OperatorBuilder::new("Differential Logging Demux".to_string(), scope.clone());

        let mut input = demux.new_input(&logs, Pipeline);

        let (mut arrangement_batches_out, arrangement_batches) = demux.new_output();
        let (mut arrangement_records_out, arrangement_records) = demux.new_output();
        let (mut sharing_out, sharing) = demux.new_output();
        let mut demux_buffer = Vec::new();
        demux.build(move |_capability| {
            move |_frontiers| {
                let arrangement_batches = arrangement_batches_out.activate();
                let arrangement_records = arrangement_records_out.activate();
                let sharing = sharing_out.activate();
                let mut arrangement_batches_session =
                    ConsolidateBuffer::new(arrangement_batches, 0);
                let mut arrangement_records_session =
                    ConsolidateBuffer::new(arrangement_records, 1);
                let mut sharing_session = ConsolidateBuffer::new(sharing, 2);

                input.for_each(|cap, data| {
                    data.swap(&mut demux_buffer);

                    for (time, worker, datum) in demux_buffer.drain(..) {
                        let time_ms = (((time.as_millis() as Timestamp / granularity_ms) + 1)
                            * granularity_ms) as Timestamp;

                        match datum {
                            DifferentialEvent::Batch(event) => {
                                arrangement_batches_session
                                    .give(&cap, ((event.operator, worker), time_ms, 1));
                                let diff = Diff::try_from(event.length).unwrap();
                                if diff != 0 {
                                    arrangement_records_session
                                        .give(&cap, ((event.operator, worker), time_ms, diff));
                                }
                            }
                            DifferentialEvent::Merge(event) => {
                                if let Some(done) = event.complete {
                                    arrangement_batches_session
                                        .give(&cap, ((event.operator, worker), time_ms, -1));
                                    let diff = Diff::try_from(done).unwrap()
                                        - Diff::try_from(event.length1 + event.length2).unwrap();
                                    if diff != 0 {
                                        arrangement_records_session
                                            .give(&cap, ((event.operator, worker), time_ms, diff));
                                    }
                                }
                            }
                            DifferentialEvent::Drop(event) => {
                                arrangement_batches_session
                                    .give(&cap, ((event.operator, worker), time_ms, -1));
                                let diff = -Diff::try_from(event.length).unwrap();
                                if diff != 0 {
                                    arrangement_records_session
                                        .give(&cap, ((event.operator, worker), time_ms, diff));
                                }
                            }
                            DifferentialEvent::MergeShortfall(_) => {}
                            DifferentialEvent::TraceShare(event) => {
                                let diff = Diff::try_from(event.diff).unwrap();
                                assert!(diff != 0);
                                sharing_session
                                    .give(&cap, ((event.operator, worker), time_ms, diff));
                            }
                        }
                    }
                });
            }
        });

        let arrangement_batches = arrangement_batches.as_collection().map({
            move |(op, worker)| {
                Row::pack_slice(&[Datum::Int64(op as i64), Datum::Int64(worker as i64)])
            }
        });

        let arrangement_records = arrangement_records.as_collection().map({
            move |(op, worker)| {
                Row::pack_slice(&[Datum::Int64(op as i64), Datum::Int64(worker as i64)])
            }
        });

        // Duration statistics derive from the non-rounded event times.
        let sharing = sharing.as_collection().map({
            move |(op, worker)| {
                Row::pack_slice(&[Datum::Int64(op as i64), Datum::Int64(worker as i64)])
            }
        });

        let logs = vec![
            (
                LogVariant::Differential(DifferentialLog::ArrangementBatches),
                arrangement_batches,
            ),
            (
                LogVariant::Differential(DifferentialLog::ArrangementRecords),
                arrangement_records,
            ),
            (LogVariant::Differential(DifferentialLog::Sharing), sharing),
        ];

        let mut result = std::collections::HashMap::new();
        for (variant, collection) in logs {
            if config.active_logs.contains_key(&variant) {
                let key = variant.index_by();
                let (_, value) = permutation_for_arrangement::<HashMap<_, _>>(
                    &key.iter()
                        .cloned()
                        .map(MirScalarExpr::Column)
                        .collect::<Vec<_>>(),
                    variant.desc().arity(),
                );
                let rows = collection.map({
                    let mut row_buf = Row::default();
                    let mut datums = DatumVec::new();
                    move |row| {
                        let datums = datums.borrow_with(&row);
                        row_buf.packer().extend(key.iter().map(|k| datums[*k]));
                        let row_key = row_buf.clone();
                        row_buf.packer().extend(value.iter().map(|c| datums[*c]));
                        let row_val = row_buf.clone();
                        (row_key, row_val)
                    }
                });

                let trace = rows
                    .arrange_named::<RowSpine<_, _, _, _>>(&format!("ArrangeByKey {:?}", variant))
                    .trace;
                result.insert(variant.clone(), (trace, Rc::clone(&token)));
            }

            if let Some(target) = config.sink_logs.get(&variant) {
                tracing::debug!("Persisting {:?} to {:?}", &variant, &target);
                persist_sink(target.0, &target.1, compute_state, &collection);
            }
        }
        result
    });

    traces
}
