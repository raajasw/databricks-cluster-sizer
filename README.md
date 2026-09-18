# Databricks cluster sizer

Six questions in, one answer out: how many workers of what type, and roughly
how long the job will take.

```
npm install
npm run dev     # http://localhost:5173
npm test
```

On Databricks you do not configure executors. You choose a worker node type and
a count, and Databricks runs one executor per worker and sizes its memory
itself. So "how many workers" **is** the executor question there, and this tool
answers that one rather than pretending you control more than you do.

---

## How the calculation works

Four numbers, multiplied together.

### 1. How much work there is

```
core-seconds = source_MB / (base_rate / format_cost / operation_cost)
```

Throughput is applied to **source bytes** — the size as stored, before
decompression — not to the expanded in-memory size. Applying it after expansion
would double-count the format, because the format multiplier below already
accounts for the same difference.

### 2. How many cores that needs

```
cores = ceil(core-seconds / target_seconds)
```

The target runtime is the main lever you control. Halving it roughly doubles
the cluster.

### 3. How many cores a worker really gives you

Databricks reserves part of each worker for its own agent and the OS, so the
tool counts **75%** of a node's cores and memory as usable.

A second limit applies on top: each operation wants a certain amount of memory
per core, and if the node has less than that, some cores must go uncounted or
the job spills to disk. A join wanting 6 GB/core on a node with 4 GB/core is
memory-bound, and the tool says so.

### 4. How many workers

```
workers = ceil(cores_needed / usable_cores_per_worker)
```

---

## Where the numbers come from

Every constant below is in [`src/simple/size.ts`](src/simple/size.ts) with the
same reasoning in a comment. They are **estimates calibrated against published
benchmarks**, not measurements of your job.

### Throughput: the softest number here

Anchored on the [TPC-DS 100 TB Full Disclosure Report][fdr] (Databricks SQL 8.3,
audited by the TPC council, November 2021):

| | |
|---|---|
| Cluster | 256 × i3.2xlarge = **2,048 vCPU** |
| Dataset | **100 TB** |
| Load time | **7,929 seconds** |
| Implied rate | **6.2 MB per core-second** |

That is world-record hardware running Photon, Databricks' native vectorized
engine. The [Photon paper (SIGMOD 2022)][photon] measures Photon against DBR,
the JVM Spark engine:

> Photon achieves a maximum speedup of 23×, and an **average speedup of 4×**
> across all queries.

So a job on the JVM engine — which is what you get without Photon, and what any
Python or Scala UDF forces regardless — runs nearer **1.5 MB per core-second**
on comparable work.

The base rates in the tool sit in that range. They are deliberately closer to
the pessimistic end: under-provisioning shows up as an out-of-memory failure
three hours into a production run, while over-provisioning costs some money.
Those are not symmetric.

A test in [`size.test.ts`](src/simple/size.test.ts) pins the resulting
ballpark against the audited figure, so drift in the constants fails the suite
rather than passing quietly.

### Format cost

How much more work a byte is to read, relative to Parquet.

| Format | Cost | Why |
|---|---|---|
| Parquet / Delta | 1× | Columnar, typed, skippable |
| Avro | 1.4× | Row-oriented but binary and typed |
| CSV | 3.5× | Parsed and typed field by field at runtime |
| JSON | 4.5× | As CSV, plus structural parsing |

Snappy-compressed Parquet compresses at [roughly 2×][snappy]; the expansion
figures the tool uses account for that plus dictionary decoding and JVM object
overhead.

### Operation cost

| Operation | Cost | Why |
|---|---|---|
| Read and filter | 1× | Streams through, nothing retained |
| Aggregate | 2.8× | Shuffles: writes to local disk, crosses the network, reads back |
| Join | 4× | Shuffles both sides, builds a hash table |
| Sort | 4.5× | Shuffles and spills a whole partition at a time |

The shuffle round trip — not the computation — is what costs the time.

### Language

| Written in | Rate vs SQL | Why |
|---|---|---|
| SQL / Scala | 1× | Runs in the JVM, Catalyst optimizes it |
| PySpark, DataFrame API | ~0.9× | Also runs in the JVM; Python only builds the plan |
| PySpark with Python UDFs | **~0.12×** | Every row leaves the JVM for a Python process and comes back |

The UDF penalty is the single biggest factor in this tool. Published benchmarks
put row-at-a-time Python UDFs at [10–20× slower][udf] than native functions; the
figure used here is at the conservative end of that range.

### Memory per core

| Operation | GB/core | Why |
|---|---|---|
| Read and filter | 2 | A partition streams through |
| Aggregate | 3 | Hash map of groups is held |
| Join | 6 | Build side plus hash table |
| Sort | 6 | A whole partition plus its sort buffers |

### Node choice

Following [Databricks' own guidance][best-practices], shuffle-heavy work
(joins, sorts, aggregations) is pointed at **storage-optimized nodes with
local NVMe**, because shuffle is disk- and network-bound more than it is
memory-bound. Plain scans get balanced nodes.

Node specifications come from the cloud vendors' published instance tables.
Each cloud has its own catalog — AWS, Azure and GCP share no instance names, so
recommending an `i3.xlarge` to an Azure workspace would simply be wrong.

---

## What this tool does not do

- **It does not model skew.** Sizing assumes data spread evenly across
  partitions. Real keys rarely are, but how badly depends on your key
  distribution, which no question here can reveal. Size for the even case, then
  check max-versus-median task duration in the Spark UI.
- **It does not read your data or your code.** Every input is a stated
  assumption.
- **It does not price anything.** Worker count and node type, not dollars.

## The honest caveat

Throughput varies enormously with the query, the data, the file layout and the
runtime. Two jobs reading the same 500 GB can differ by an order of magnitude.
This tool gets you a defensible starting point and shows its arithmetic so you
can argue with it — then run the job once and correct from what you actually
see.

Every tool in this space has the same limitation. The ones that do not show
their workings simply hide it.

[fdr]: https://www.tpc.org/results/fdr/tpcds/databricks~tpcds~100000~databricks_sql_8.3~fdr~2021-11-02~v01.pdf
[photon]: https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf
[best-practices]: https://docs.databricks.com/aws/en/compute/cluster-config-best-practices
[udf]: https://medium.com/quantumblack/spark-udf-deep-insights-in-performance-f0a95a4d8c62
[snappy]: https://parquet.apache.org/docs/file-format/data-pages/compression/
