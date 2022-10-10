# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys
import time
from dataclasses import dataclass
from textwrap import dedent
from typing import Callable, Optional

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Computed,
    Kafka,
    Localstack,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Materialized(),
    Testdrive(volumes=["mzdata:/mzdata"]),
]


class CompactionCheck:
    # replica: a string describing the SQL accessible name of the replica. Example"cluster1.replica1"
    # host: docker container name from which to check the log. Example: "computed_1_1"
    def __init__(self, replica: str, host: str):
        assert "." in replica
        self.replica = replica
        self.host = host
        self.id: Optional[str] = None
        self.satisfied = False

    def find_id(self, c: Composition) -> None:
        assert False

    def print_error(self) -> None:
        assert False

    def check_log(self, c: Composition) -> None:
        self.find_id(c)
        assert self.id is not None
        log: str = c.invoke("logs", self.host, capture=True).stdout
        for line in [x for x in log.splitlines() if x.find("ClusterClient send=") > -1]:
            if "AllowCompaction" in line:
                if self.id in line:
                    self.satisfied = True
        if not self.satisfied:
            self.print_error()

    def replica_id(self, c: Composition) -> str:
        cursor = c.sql_cursor()
        (cluster, replica) = self.replica.split(".")
        cursor.execute(
            f"""
                SELECT mz_cluster_replicas.id FROM mz_clusters, mz_cluster_replicas
                WHERE cluster_id = mz_clusters.id AND mz_clusters.name = '{cluster}'
                AND mz_cluster_replicas.name = '{replica}'""",
        )
        return cursor.fetchone()[0]

    def cluster_id(self, c: Composition) -> str:
        cursor = c.sql_cursor()
        (cluster, replica) = self.replica.split(".")
        cursor.execute(
            f"SELECT id FROM mz_clusters WHERE mz_clusters.name = '{cluster}'",
        )
        return cursor.fetchone()[0]

    @staticmethod
    def _format_id(iid: str) -> str:
        if iid[0] == "s":
            return "System(" + iid[1:] + ")"
        if iid[0] == "u":
            return "User(" + iid[1:] + ")"
        assert False

    @staticmethod
    def all_checks(replica: str, host: str) -> list["CompactionCheck"]:
        return [
            # PersistedIntro(replica, host),
            Mv(replica, host),
            ArrangedIntro(replica, host),
        ]


class PersistedIntro(CompactionCheck):
    # TODO: AllowCompactions are currently not sent for persisted introspection. Its unclear
    # if they should arrive or not
    # This test is disabled.
    def __init__(self, replica: str, host: str) -> None:
        return super().__init__(replica, host)

    def find_id(self, c: Composition) -> None:
        cursor = c.sql_cursor()
        replica_id = self.replica_id(c)

        # Get a persisted introspection id
        cursor.execute(
            f"""
                SELECT id,shard_id from mz_internal.mz_storage_shards,mz_catalog.mz_sources
                WHERE object_id = id AND name = 'mz_scheduling_elapsed_internal_{replica_id}'
            """
        )
        self.id = CompactionCheck._format_id(cursor.fetchone()[0])

    def print_error(self) -> None:
        print(
            f"!! AllowCompaction not found for persisted introspection with id {self.id}"
        )


class Mv(CompactionCheck):
    def __init__(self, replica: str, host: str) -> None:
        return super().__init__(replica, host)

    def find_id(self, c: Composition) -> None:
        cursor = c.sql_cursor()
        cursor.execute(
            """
                SELECT id,shard_id from mz_internal.mz_storage_shards, mz_catalog.mz_materialized_views
                WHERE object_id = id AND name = 'v3';
            """
        )
        self.id = CompactionCheck._format_id(cursor.fetchone()[0])

    def print_error(self) -> None:
        print(f"!! AllowCompaction not found for materialized view with id {self.id}")


class ArrangedIntro(CompactionCheck):
    def __init__(self, replica: str, host: str) -> None:
        return super().__init__(replica, host)

    def find_id(self, c: Composition) -> None:
        # Get the arranged introspection id, no shard id for those
        cluster_id = self.cluster_id(c)
        cursor = c.sql_cursor()
        cursor.execute(
            f"""
                SELECT idx.id from mz_catalog.mz_sources AS src, mz_catalog.mz_indexes AS idx
                WHERE src.name = 'mz_scheduling_elapsed_internal'
                AND src.id = idx.on_id AND idx.cluster_id = {cluster_id}"""
        )
        self.id = CompactionCheck._format_id(cursor.fetchone()[0])

    def print_error(self) -> None:
        print(
            f"!! AllowCompaction not found for arranged introspection with id {self.id}"
        )


def populate(c: Composition) -> None:
    # Create some database objects
    c.testdrive(
        dedent(
            """
            > CREATE TABLE t1 (f1 INTEGER);
            > INSERT INTO t1 SELECT * FROM generate_series(1, 10);
            > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c1 FROM t1;
            > CREATE TABLE ten (f1 INTEGER);
            > INSERT INTO ten SELECT * FROM generate_series(1, 10);
            > CREATE MATERIALIZED VIEW expensive AS SELECT (a1.f1 * 1) +
              (a2.f1 * 10) +
              (a3.f1 * 100) +
              (a4.f1 * 1000) +
              (a5.f1 * 10000) +
              (a6.f1 * 100000) +
              (a7.f1 * 1000000)
              FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6, ten AS a7;
            $ kafka-create-topic topic=source1
            $ kafka-ingest format=bytes topic=source1 repeat=1000000
            A${kafka-ingest.iteration}
            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${testdrive.kafka-addr}')
            > CREATE SOURCE source1
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-source1-${testdrive.seed}')
              FORMAT BYTES
            > CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) FROM source1
            """
        ),
    )


def restart_replica(c: Composition) -> None:
    c.kill("computed_1_1", "computed_1_2")
    c.up("computed_1_1", "computed_1_2")


def drop_create_replica(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
            > DROP CLUSTER REPLICA cluster1.replica1
            > CREATE CLUSTER REPLICA cluster1.replica3
              REMOTE ['computed_1_1:2100', 'computed_1_2:2100'],
              COMPUTE ['computed_1_1:2102', 'computed_1_2:2102']
            """
        )
    )


def create_invalid_replica(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
            > CREATE CLUSTER REPLICA cluster1.replica3
              REMOTE ['no_such_host:2100'],
              COMPUTE ['no_such_host:2102']
            """
        )
    )


def validate(c: Composition) -> None:
    # Validate that the cluster continues to operate
    c.testdrive(
        dedent(
            """
            # Dataflows

            > SELECT * FROM v1;
            10

            # Existing sources
            $ kafka-ingest format=bytes topic=source1 repeat=1000000
            B${kafka-ingest.iteration}
            > SELECT * FROM v2;
            2000000

            # Existing tables
            > INSERT INTO t1 VALUES (20);
            > SELECT * FROM v1;
            11

            # New materialized views
            > CREATE MATERIALIZED VIEW v3 AS SELECT COUNT(*) AS c1 FROM t1;
            > SELECT * FROM v3;
            11

            # New tables
            > CREATE TABLE t2 (f1 INTEGER);
            > INSERT INTO t2 SELECT * FROM t1;
            > SELECT COUNT(*) FROM t2;
            11

            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${testdrive.kafka-addr}')

            # New sources
            > CREATE SOURCE source2
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-source1-${testdrive.seed}')
              FORMAT BYTES
            > SELECT COUNT(*) FROM source2
            2000000
"""
        ),
    )


def validate_introspection_compaction(
    c: Composition, checks: list[CompactionCheck]
) -> None:
    # Validate that the AllowCompaction commands arrive at the corresponding replicas.

    # Sleep a bit to give envd a chance to produce AllowCompactions
    time.sleep(5)

    for check in checks:
        check.check_log(c)

    for check in checks:
        if not check.satisfied:
            sys.exit(-1)


@dataclass
class Disruption:
    name: str
    disruption: Callable
    compaction_checks: list[CompactionCheck]


disruptions = [
    Disruption(
        name="none",
        disruption=lambda c: None,
        compaction_checks=CompactionCheck.all_checks(
            "cluster1.replica1", "computed_1_1"
        )
        + CompactionCheck.all_checks("cluster1.replica2", "computed_2_1"),
    ),
    Disruption(
        name="drop-create-replica",
        disruption=lambda c: drop_create_replica(c),
        # With a disfunctional replica, we don't expect AllowCompactions for materialized views.
        compaction_checks=[
            # PersistedIntro("cluster1.replica2", "computed_2_1"),
            ArrangedIntro("cluster1.replica2", "computed_2_1"),
        ],
    ),
    Disruption(
        name="create-invalid-replica",
        disruption=lambda c: create_invalid_replica(c),
        # With a disfunctional replica, we don't expect AllowCompactions for materialized views.
        compaction_checks=[
            # PersistedIntro("cluster1.replica2", "computed_2_1"),
            ArrangedIntro("cluster1.replica2", "computed_2_1"),
        ],
    ),
    Disruption(
        name="restart-replica",
        disruption=lambda c: restart_replica(c),
        compaction_checks=CompactionCheck.all_checks(
            "cluster1.replica1", "computed_1_1"
        )
        + CompactionCheck.all_checks("cluster1.replica2", "computed_2_1"),
    ),
    Disruption(
        name="pause-one-computed",
        disruption=lambda c: c.pause("computed_1_1"),
        # With a disfunctional replica, we don't expect AllowCompactions for materialized views.
        compaction_checks=[
            # PersistedIntro("cluster1.replica2", "computed_2_1"),
            ArrangedIntro("cluster1.replica2", "computed_2_1"),
        ],
    ),
    Disruption(
        name="kill-replica",
        disruption=lambda c: c.kill("computed_1_1", "computed_1_2"),
        # With a disfunctional replica, we don't expect AllowCompactions for materialized views.
        compaction_checks=[
            # PersistedIntro("cluster1.replica2", "computed_2_1"),
            ArrangedIntro("cluster1.replica2", "computed_2_1"),
        ],
    ),
    Disruption(
        name="drop-replica",
        disruption=lambda c: c.testdrive("> DROP CLUSTER REPLICA cluster1.replica1"),
        compaction_checks=CompactionCheck.all_checks(
            "cluster1.replica2", "computed_2_1"
        ),
    ),
]


def workflow_default(c: Composition) -> None:
    """Test replica isolation by introducing faults of various kinds in replica1
    and then making sure that the cluster continues to operate properly
    """

    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "localstack"]
    )
    for id, disruption in enumerate(disruptions):
        run_test(c, disruption, id)


def run_test(c: Composition, disruption: Disruption, id: int) -> None:
    print(f"+++ Running disruption scenario {disruption.name}")

    c.up("testdrive", persistent=True)

    logging_env = ["COMPUTED_LOG_FILTER=mz_compute::server=debug,info"]
    nodes = [
        Computed(name="computed_1_1", environment=logging_env),
        Computed(name="computed_1_2", environment=logging_env),
        Computed(name="computed_2_1", environment=logging_env),
        Computed(name="computed_2_2", environment=logging_env),
    ]

    with c.override(*nodes):
        c.up("materialized", *[n.name for n in nodes])
        c.wait_for_materialized()

        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                replica1 (
                    REMOTE ['computed_1_1:2100', 'computed_1_2:2100'],
                    COMPUTE ['computed_1_1:2102', 'computed_1_2:2102']
                    ),
                replica2 (
                    REMOTE ['computed_2_1:2100', 'computed_2_2:2100'],
                    COMPUTE ['computed_2_1:2102', 'computed_2_2:2102']
                    )
            )
            """
        )

        with c.override(
            Testdrive(
                validate_data_dir=False,
                no_reset=True,
                materialize_params={"cluster": "cluster1"},
                seed=id,
            )
        ):
            populate(c)

            # Disrupt replica1 by some means
            disruption.disruption(c)

            validate(c)

            validate_introspection_compaction(c, disruption.compaction_checks)

        cleanup_list = ["materialized", "testdrive", *[n.name for n in nodes]]
        c.kill(*cleanup_list)
        c.rm(*cleanup_list, destroy_volumes=True)
        c.rm_volumes("mzdata", "pgdata")
