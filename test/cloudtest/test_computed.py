# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest
from pg8000.exceptions import InterfaceError
from kubernetes.client import V1StatefulSet, V1EnvVar

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.exists import exists, not_exists
from materialize.cloudtest.wait import wait
from materialize.cloudtest.k8s import K8sLoadedStatefulSet

def get_cluster_id(mz : MaterializeApplication, cluster : str, replica : str) -> tuple[str, str]:
    (cluster_id, replica_id) = mz.environmentd.sql_query(
        f"SELECT cluster_id, mz_cluster_replicas.id FROM mz_cluster_replicas, mz_clusters WHERE cluster_id = mz_clusters.id AND mz_clusters.name = '{cluster}' AND mz_cluster_replicas.name = '{replica}'"
    )[0]
    assert cluster_id is not None
    assert replica_id is not None
    return (cluster_id, replica_id)


def test_computed_sizing(mz: MaterializeApplication) -> None:
    """Test that a SIZE N cluster indeed creates N computed instances."""
    SIZE = 2

    mz.environmentd.sql(
        f"CREATE CLUSTER sized1 REPLICAS (sized_replica1 (SIZE '{SIZE}-1'))"
    )

    (cluster_id, replica_id) = get_cluster_id(mz, "sized1", "sized_replica1")

    for compute_id in range(0, SIZE):
        compute_pod = (
            f"pod/compute-cluster-{cluster_id}-replica-{replica_id}-{compute_id}"
        )
        wait(condition="condition=Ready", resource=compute_pod)

    mz.environmentd.sql("DROP CLUSTER sized1 CASCADE")

def test_computed_recovery(mz: MaterializeApplication) -> None:
    """ Test that a stale envd that instantiate an out-of-date computed will eventually get replaced
    with the correct version and the system stabilizes """

    mz.environmentd.sql("CREATE CLUSTER c1 REPLICAS (r1 (SIZE '1'))")
    mz.environmentd.sql("CREATE TABLE t1 (a int)")
    mz.environmentd.sql("INSERT INTO t1 VALUES (1)")
    (cluster_id, replica_id) = get_cluster_id(mz, "c1", "r1")

    # wait until computed is up
    compute_pod = (
        f"pod/compute-cluster-{cluster_id}-replica-{replica_id}-0"
    )
    wait(condition="condition=Ready", resource=compute_pod)

    ss = K8sLoadedStatefulSet(f"compute-cluster-{cluster_id}-replica-{replica_id}")
    ss.set_failpoints("grpc_version_check=return(error)")

    mz.environmentd.sql("SET CLUSTER TO c1; SELECT max(a) FROM t1;")


@pytest.mark.parametrize(
    "failpoint",
    ["", "after_catalog_drop_replica=panic", "after_sequencer_drop_replica=panic"],
)
def test_computed_shutdown(mz: MaterializeApplication, failpoint: str) -> None:
    """Test that dropping a cluster or replica causes the associated computeds to shut down."""

    print(f"Testing computed shutdown with failpoint={failpoint}")

    mz.set_environmentd_failpoints(failpoint)

    def sql_expect_crash(sql: str) -> None:
        # We expect executing `sql` will crash environmentd. To ensure it is actually `sql`
        # wait until the SQL interface is available.
        mz.wait_for_sql()
        try:
            mz.environmentd.sql(sql)
        except InterfaceError as e:
            print(f"Expected SQL error: {e}")

    mz.environmentd.sql(
        "CREATE CLUSTER shutdown1 REPLICAS (shutdown_replica1 (SIZE '1'), shutdown_replica2 (SIZE '1'))"
    )

    cluster_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_clusters WHERE name = 'shutdown1'"
    )[0][0]
    assert cluster_id is not None

    compute_pods = {}
    compute_svcs = {}
    for replica_name in ["shutdown_replica1", "shutdown_replica2"]:
        replica_id = mz.environmentd.sql_query(
            f"SELECT id FROM mz_cluster_replicas WHERE name = '{replica_name}'"
        )[0][0]
        assert replica_id is not None

        compute_pod = f"pod/compute-cluster-{cluster_id}-replica-{replica_id}-0"
        compute_pods[replica_name] = compute_pod
        wait(condition="condition=Ready", resource=compute_pod)

        compute_svc = f"service/compute-cluster-{cluster_id}-replica-{replica_id}"
        compute_svcs[replica_name] = compute_svc
        exists(resource=compute_svc)

    sql_expect_crash("DROP CLUSTER REPLICA shutdown1.shutdown_replica1")
    wait(condition="delete", resource=compute_pods["shutdown_replica1"])
    not_exists(resource=compute_svcs["shutdown_replica1"])

    sql_expect_crash("DROP CLUSTER shutdown1 CASCADE")
    wait(condition="delete", resource=compute_pods["shutdown_replica2"])
    not_exists(resource=compute_svcs["shutdown_replica2"])

    mz.set_environmentd_failpoints("")
