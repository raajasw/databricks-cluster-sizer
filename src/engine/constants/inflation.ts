/**
 * In-memory inflation: how much larger data gets when read off disk.
 *
 * There is no single "inflation factor", and tools that report one are lying.
 * It decomposes into four independent multipliers:
 *
 *   1. CODEC       -- undoing snappy/zstd/gzip block compression
 *   2. ENCODING    -- undoing Parquet dictionary/RLE/bit-packing. Usually the
 *                     DOMINANT term and almost always ignored: a dictionary-
 *                     encoded low-cardinality string column can be 20-50x
 *                     larger once materialized as individual UTF8String values.
 *   3. REPRESENTATION -- Spark's UnsafeRow is compact (~1.2-1.5x), but
 *                     deserializing to JVM objects or crossing into Python
 *                     multiplies again.
 *   4. AMPLIFICATION -- peak working set is not the dataset; it is the sort
 *                     buffers, hash maps and join build sides live at once.
 *
 * Each is a Range. The product is deliberately wide -- typically 3-4x between
 * low and high -- and that width is the honest answer.
 */

import { fromBounds, point, type Range } from '../range';
import type { Codec, StorageFormat, RuntimeLanguage, QueryShape } from '../types/input';

/**
 * Factor 1: block-compression ratio, for ROW-oriented formats only.
 *
 * For columnar formats this is NOT used on its own -- see COLUMNAR_DECODE_FACTOR
 * below for why multiplying codec x encoding double-counts.
 */
export const CODEC_FACTOR: Record<Codec, Range> = {
  none: point(1),
  lz4: fromBounds(1.8, 3.5),
  snappy: fromBounds(2.0, 4.0),
  zstd: fromBounds(3.0, 6.0),
  gzip: fromBounds(4.0, 8.0),
  unknown: fromBounds(2.0, 6.0),
};

/**
 * Factor 2, per column class: cost of undoing columnar encodings.
 *
 * Low-cardinality strings are the headline case. On disk a dictionary-encoded
 * status column is a handful of distinct values plus tiny integer codes; in
 * memory it is one UTF8String per row. Ratios of 20-50x are ordinary.
 * High-cardinality strings barely compressed in the first place, so undoing it
 * costs little.
 */
/**
 * Combined disk-to-memory decode factor for COLUMNAR formats.
 *
 * A Parquet file's snappy ratio is measured on data that is ALREADY dictionary-
 * and RLE-encoded. Multiplying a codec factor by an encoding factor therefore
 * counts the same dictionary savings twice: it is the single easiest way to
 * produce a wildly overestimated memory requirement, and it is why a naive
 * model reports 2 GB of Parquet inflating to 40 GB.
 *
 * What the combined factor actually captures is the ratio between compressed
 * on-disk bytes and Spark's in-memory UnsafeRow representation. Empirically
 * that lands around 3-8x for typical Parquet, skewed higher when
 * low-cardinality string columns dominate the row (their dictionary collapses
 * hard on disk and expands to one UTF8String per row in memory) and lower for
 * numeric-heavy data where on-disk bit-packing is already close to the
 * in-memory width.
 */
export const COLUMNAR_DECODE_FACTOR_BY_COLUMN_CLASS = {
  numeric: fromBounds(2.0, 4.0),
  lowCardString: fromBounds(6.0, 18.0),
  highCardString: fromBounds(2.5, 5.0),
  nested: fromBounds(4.0, 9.0),
} as const;

/** Used when the column mix is unknown. Wide on purpose. */
export const COLUMNAR_DECODE_FACTOR_UNKNOWN: Range = fromBounds(3.0, 8.0);

export const ENCODING_FACTOR_BY_COLUMN_CLASS = {
  numeric: fromBounds(1.1, 1.5),
  lowCardString: fromBounds(4.0, 20.0),
  highCardString: fromBounds(1.2, 2.0),
  nested: fromBounds(2.0, 4.0),
} as const;

export type ColumnClass = keyof typeof ENCODING_FACTOR_BY_COLUMN_CLASS;

/** Used when the user has not described their column mix. Wide on purpose. */
export const ENCODING_FACTOR_UNKNOWN: Range = fromBounds(2.5, 6.0);

/** Row-oriented formats carry no columnar encoding to undo. */
export const FORMAT_HAS_COLUMNAR_ENCODING: Record<StorageFormat, boolean> = {
  parquet: true,
  delta: true,
  iceberg: true,
  orc: true,
  avro: false,
  json: false,
  csv: false,
  text: false,
};

/** Only columnar formats can skip unread columns at the scan. */
export const FORMAT_SUPPORTS_PROJECTION_PUSHDOWN = FORMAT_HAS_COLUMNAR_ENCODING;

/**
 * Non-splittable codecs force a whole file into ONE task, regardless of cluster
 * size. gzip is the notorious case: a 40 GB .csv.gz is single-threaded.
 */
export const CODEC_IS_SPLITTABLE: Record<Codec, boolean> = {
  none: true,
  lz4: true,
  snappy: true,
  zstd: true,
  gzip: false,
  unknown: true,
};

/**
 * Factor 3: how rows are represented once in the executor.
 *
 * Spark SQL on UnsafeRow is cheap. Python UDFs are not: rows are pickled,
 * shipped over a socket to a worker process, and materialized again on the far
 * side, so peak memory counts both copies.
 */
export const REPRESENTATION_FACTOR: Record<RuntimeLanguage, Range> = {
  'scala-java': fromBounds(1.2, 1.5),
  'pyspark-sql-only': fromBounds(1.2, 1.6),
  'pyspark-udf': fromBounds(2.5, 5.0),
  'pyspark-pandas-udf': fromBounds(2.0, 4.0),
  sparkr: fromBounds(2.5, 5.0),
};

/**
 * Factor 4: peak working set relative to the scanned data, by query shape.
 *
 * A pure scan-and-write streams through with almost no accumulation. A
 * sort-merge join holds sort buffers for both sides plus the shuffle output.
 */
export const QUERY_AMPLIFICATION: Record<QueryShape, Range> = {
  'scan-filter-write': point(1.0),
  'narrow-transform': fromBounds(1.0, 1.3),
  aggregation: fromBounds(1.3, 2.0),
  sort: fromBounds(1.5, 2.5),
  'shuffle-join': fromBounds(2.0, 3.0),
  'broadcast-join': fromBounds(1.2, 1.8),
  window: fromBounds(1.5, 2.5),
  'iterative-ml': fromBounds(1.5, 3.0),
  'multi-stage-dag': fromBounds(2.0, 3.5),
};

/**
 * Caching a DataFrame does NOT store the inflated form. Spark SQL caches into
 * an InMemoryRelation using compressed columnar storage
 * (spark.sql.inMemoryColumnarStorage.compressed, default true), so a cached
 * DataFrame is frequently SMALLER than its deserialized size. Getting this
 * backwards oversizes clusters badly.
 */
export const DATAFRAME_CACHE_FACTOR: Range = fromBounds(0.3, 0.7);

/** RDD MEMORY_ONLY caching keeps live JVM objects -- no such saving. */
export const RDD_CACHE_FACTOR: Range = fromBounds(1.0, 1.4);

/**
 * Shuffle writes are compressed (spark.shuffle.compress, lz4 by default), so
 * bytes hitting local disk are well under the in-memory size.
 */
export const SHUFFLE_COMPRESSION_FACTOR: Range = fromBounds(0.3, 0.5);

/**
 * Spilling is not a straight write of the overflow: the sorter writes runs and
 * then merges them, so bytes touching disk exceed the spilled volume.
 */
export const SPILL_WRITE_AMPLIFICATION: Range = fromBounds(1.5, 2.0);
