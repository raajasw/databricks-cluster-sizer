/**
 * s01 effectiveDataSize and s02 inflation.
 */

import { bytes, type Bytes } from '../../units';
import { point, mulIndependent, mulRange, type Range } from '../../range';
import {
  CODEC_FACTOR, COLUMNAR_DECODE_FACTOR_BY_COLUMN_CLASS, COLUMNAR_DECODE_FACTOR_UNKNOWN,
  FORMAT_HAS_COLUMNAR_ENCODING, FORMAT_SUPPORTS_PROJECTION_PUSHDOWN,
  REPRESENTATION_FACTOR, QUERY_AMPLIFICATION,
} from '../../constants/inflation';
import type { ColumnMix, Codec } from '../../types/input';
import type { InflationBreakdown } from '../../types/output';
import { makeStep, fmtBytes, fmtRange, fmtX } from '../trace';
import { withStep, withScratch, type NamedStage, type PipelineContext } from '../context';

export const SCANNED_BYTES = 'scannedBytesOnDisk';
export const INFLATION = 'inflation';
export const INFLATED_BYTES = 'inflatedScannedBytes';

/** s01: logical input size -> bytes actually scanned, after pushdown. */
export const effectiveDataSize: NamedStage = {
  name: 'effectiveDataSize',
  run(ctx: PipelineContext): PipelineContext {
    const { data } = ctx.input;
    const columnar = FORMAT_SUPPORTS_PROJECTION_PUSHDOWN[data.format];

    // Projection pushdown only exists on columnar formats. Reading 3 of 200
    // columns from CSV still reads every byte of every row.
    const colFrac = columnar ? (data.columnsReadFraction ?? 1) : 1;
    const rowFrac = data.rowsSurvivingFilterFraction ?? 1;

    const scanned = bytes(data.logicalBytesOnDisk * colFrac * rowFrac);

    const notes: string[] = [];
    if (!columnar && (data.columnsReadFraction ?? 1) < 1) {
      notes.push(
        `${data.format} is row-oriented, so column pruning saves no I/O: the whole row is read and unused columns are discarded after parsing.`,
      );
    }

    const step = makeStep({
      id: 'effective-data-size',
      title: 'Bytes actually scanned',
      formula:
        `${fmtBytes(data.logicalBytesOnDisk)} x ${colFrac} (columns read) ` +
        `x ${rowFrac} (rows surviving filters) = ${fmtBytes(scanned)}`,
      inputs: {
        logicalBytesOnDisk: data.logicalBytesOnDisk,
        format: data.format,
        columnsReadFraction: colFrac,
        rowsSurvivingFilterFraction: rowFrac,
      },
      outputs: { scannedBytesOnDisk: scanned },
      rationale:
        (columnar
          ? 'Columnar formats skip unread columns at the scan, and row-group min/max statistics let Spark skip whole row groups. '
          : 'This is a row-oriented format, so unread columns cost full I/O. ') +
        notes.join(' '),
      confidence: 'estimated',
      citations: [
        { label: 'spark.sql.parquet.filterPushdown', kind: 'spark-config' },
      ],
    });

    return withScratch(withStep(ctx, step), { [SCANNED_BYTES]: scanned });
  },
};

/**
 * The disk-to-memory decode factor.
 *
 * For columnar formats this is ONE factor, not codec x encoding: a Parquet
 * file's compression ratio is measured on already-encoded data, so treating
 * them as independent double-counts the dictionary savings.
 */
function decodeFactorFor(
  mix: ColumnMix | undefined,
  columnar: boolean,
  codec: Codec,
): { factor: Range; hint?: string; label: string } {
  if (!columnar) {
    // Row formats have no columnar encoding, so the codec ratio stands alone.
    return { factor: CODEC_FACTOR[codec], label: 'codec' };
  }
  if (!mix) {
    return {
      factor: COLUMNAR_DECODE_FACTOR_UNKNOWN,
      hint: 'Describe your column type mix to narrow this estimate.',
      label: 'decode',
    };
  }
  const total = mix.numeric + mix.lowCardString + mix.highCardString + mix.nested;
  if (total <= 0) return { factor: COLUMNAR_DECODE_FACTOR_UNKNOWN, label: 'decode' };

  const w = {
    numeric: mix.numeric / total,
    lowCardString: mix.lowCardString / total,
    highCardString: mix.highCardString / total,
    nested: mix.nested / total,
  };
  const blend = (pick: (r: Range) => number): number =>
    w.numeric * pick(COLUMNAR_DECODE_FACTOR_BY_COLUMN_CLASS.numeric) +
    w.lowCardString * pick(COLUMNAR_DECODE_FACTOR_BY_COLUMN_CLASS.lowCardString) +
    w.highCardString * pick(COLUMNAR_DECODE_FACTOR_BY_COLUMN_CLASS.highCardString) +
    w.nested * pick(COLUMNAR_DECODE_FACTOR_BY_COLUMN_CLASS.nested);

  return {
    factor: {
      low: blend((r) => r.low),
      mid: blend((r) => r.mid),
      high: blend((r) => r.high),
    },
    label: 'decode',
  };
}

/** s02: the four-factor inflation decomposition. */
export const inflation: NamedStage = {
  name: 'inflation',
  run(ctx: PipelineContext): PipelineContext {
    const { data, runtimeLanguage, pipeline } = ctx.input;
    const columnar = FORMAT_HAS_COLUMNAR_ENCODING[data.format];

    const { factor: decodeFactor, hint, label: decodeLabel } =
      decodeFactorFor(data.columnMix, columnar, data.codec);
    const representationFactor = REPRESENTATION_FACTOR[runtimeLanguage];
    const queryAmplification = QUERY_AMPLIFICATION[pipeline.dominantQueryShape];

    // Independent factors: combined in quadrature rather than by multiplying
    // extremes, which would compound four simultaneous worst cases.
    const total = mulIndependent(
      decodeFactor,
      representationFactor,
      queryAmplification,
    );

    const breakdown: InflationBreakdown = {
      codecFactor: columnar ? point(1) : decodeFactor,
      encodingFactor: columnar ? decodeFactor : point(1),
      representationFactor,
      queryAmplification,
      total,
      confidence: data.columnMix ? 'estimated' : 'guess',
      tighteningHint: hint,
    };

    const scanned = ctx.draft.scratch[SCANNED_BYTES] as Bytes;
    const inflated = mulRange(point(scanned), total);

    const step = makeStep({
      id: 'inflation',
      title: 'In-memory inflation',
      formula:
        `${decodeLabel} ${fmtX(decodeFactor)} x representation ${fmtX(representationFactor)} ` +
        `x query ${fmtX(queryAmplification)} = ${fmtX(total)}  ->  ` +
        `${fmtBytes(scanned)} becomes ${fmtRange(inflated, 'bytes')}`,
      inputs: {
        scannedBytesOnDisk: scanned,
        codec: data.codec,
        format: data.format,
        runtimeLanguage,
        queryShape: pipeline.dominantQueryShape,
      },
      outputs: {
        decodeFactor, representationFactor,
        queryAmplification, totalInflation: total, inflatedBytes: inflated,
      },
      rationale:
        (columnar
          ? 'The decode factor covers decompression AND undoing dictionary, run-length and ' +
            'bit-packing encoding together, because they cannot be separated: a Parquet ' +
            'file\'s compression ratio is already measured on encoded data, so treating them ' +
            'as two independent multipliers double-counts the same saving. Low-cardinality ' +
            'string columns drive this highest -- a handful of dictionary entries on disk ' +
            'becomes one string object per row in memory.'
          : 'This is a row-oriented format, so decompression is the whole story at the scan.') +
        ' Representation covers how rows are held once decoded, and query amplification ' +
        'covers the sort buffers, hash maps and join build sides live at peak. ' +
        'The factors are combined in quadrature rather than multiplied end to end: they are ' +
        'largely independent, and compounding every worst case at once produces a range too ' +
        'wide to act on.',
      confidence: breakdown.confidence,
      citations: [
        { label: 'Parquet dictionary & RLE encoding', kind: 'vendor-doc' },
        { label: 'spark.sql.inMemoryColumnarStorage.compressed', kind: 'spark-config' },
      ],
      alternatives: !data.columnMix
        ? [{
            value: COLUMNAR_DECODE_FACTOR_UNKNOWN,
            whyRejected:
              'Using a generic decode factor because no column mix was supplied; ' +
              'supplying one narrows the range materially.',
          }]
        : undefined,
    });

    return withScratch(withStep(ctx, step), {
      [INFLATION]: breakdown,
      [INFLATED_BYTES]: inflated,
    });
  },
};
