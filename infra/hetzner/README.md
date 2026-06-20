# infra/hetzner/ — the only cloud-aware layer

Following the spark-k8s convention, **this is the one place that knows which cloud we're
on.** Everything in `ansible/`, `producer/`, `streaming/` and `web/` is provider-neutral.

## Reuse mode (default)

market-stream does **not** provision infrastructure. It deploys its workloads onto the
**existing spark-k8s Hetzner cluster** and writes into that cluster's Lakekeeper warehouse.
So this directory is intentionally thin: there is nothing to `tofu apply` here. The cluster
is created from the [`spark-k8s`](../../../spark-k8s) repo:

```bash
cd ../spark-k8s/infra/hetzner && tofu apply        # nodes + S3 warehouse bucket
cd ../../ansible && ansible-playbook site.yml      # k8s + Lakekeeper + Spark Operator
```

market-stream then reuses that cluster's rendered inventory:

```bash
cp ../spark-k8s/ansible/inventory/hosts.ini ../../ansible/inventory/hosts.ini
cd ../../ && make deploy
```

## Standalone mode (documented extension, not built)

To make market-stream fully self-contained on a fresh cluster, add an `infra/hetzner/*.tf`
here that mirrors `spark-k8s/infra/hetzner/` — provision the nodes + an S3 bucket and render
`ansible/inventory/hosts.ini` in the **same contract format** (`control_plane`/`workers`
groups, `node_private_ip`, and the `object_storage_*` vars). Nothing in `ansible/` changes;
that's the whole point of the seam. Supporting another cloud is likewise just another
`infra/<cloud>/` that emits the same inventory.
