/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * A map/reduce program that uses Bailey-Borwein-Plouffe to compute exact 
 * digits of Pi.
 * This program is able to calculate digit positions
 * lower than a certain limit, which is roughly 10^8.
 * If the limit is exceeded,
 * the corresponding results may be incorrect due to overflow errors.
 * For computing higher bits of Pi, consider using distbbp. 
 * 
 * Reference:
 *
 * [1] David H. Bailey, Peter B. Borwein and Simon Plouffe.  On the Rapid
 *     Computation of Various Polylogarithmic Constants.
 *     Math. Comp., 66:903-913, 1996.
 */
public class BaileyBorweinPlouffe extends Configured implements Tool {
  public static final String DESCRIPTION
      = "A map/reduce program that uses Bailey-Borwein-Plouffe to compute exact digits of Pi.";

  private static final String NAME = "mapreduce." + 
    BaileyBorweinPlouffe.class.getSimpleName();

  //custom job properties
  private static final String WORKING_DIR_PROPERTY = NAME + ".dir";
  private static final String HEX_FILE_PROPERTY = NAME + ".hex.file";
  private static final String DIGIT_START_PROPERTY = NAME + ".digit.start";
  private static final String DIGIT_SIZE_PROPERTY = NAME + ".digit.size";
  private static final String DIGIT_PARTS_PROPERTY = NAME + ".digit.parts";

  private static final Logger LOG =
      LoggerFactory.getLogger(BaileyBorweinPlouffe.class);

  /** Mapper class computing digits of Pi. */
  public static class BbpMapper extends
      Mapper<LongWritable, IntWritable, LongWritable, BytesWritable> {

    /** Compute the (offset+1)th to (offset+length)th digits. */
    protected void map(LongWritable offset, IntWritable length,
        final Context context) throws IOException, InterruptedException {
      LOG.info("offset=" + offset + ", length=" + length);

      // compute digits
      final byte[] bytes = new byte[length.get() >> 1];
      long d = offset.get();
      for (int i = 0; i < bytes.length; d += 4) {
        final long digits = hexDigits(d);
        bytes[i++] = (byte) (digits >> 8);
        bytes[i++] = (byte) digits;
      }

      // output map results
      context.write(offset, new BytesWritable(bytes));
    }
  }

  /** Reducer for concatenating map outputs. */
  public static class BbpReducer extends
      Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {

    /** Storing hex digits */
    private final List<Byte> hex = new ArrayList<Byte>();

    /** Concatenate map outputs. */
    @Override
    protected void reduce(LongWritable offset, Iterable<BytesWritable> values,
        Context context) throws IOException, InterruptedException {
      // read map outputs
      for (BytesWritable bytes : values) {
        for (int i = 0; i < bytes.getLength(); i++)
          hex.add(bytes.getBytes()[i]);
      }

      LOG.info("hex.size() = " + hex.size());
    }

    /** Write output to files. */
    @Override
    protected void cleanup(Context context
        ) throws IOException, InterruptedException {
      final Configuration conf = context.getConfiguration();
      final Path dir = new Path(conf.get(WORKING_DIR_PROPERTY));
      final FileSystem fs = dir.getFileSystem(conf);

      // write hex output
      {
        final Path hexfile = new Path(conf.get(HEX_FILE_PROPERTY));
        final OutputStream out = new BufferedOutputStream(fs.create(hexfile));
        try {
          for (byte b : hex)
            out.write(b);
        } finally {
          out.close();
        }
      }

      // If the starting digit is 1,
      // the hex value can be converted to decimal value.
      if (conf.getInt(DIGIT_START_PROPERTY, 1) == 1) {
        final Path outfile = new Path(dir, "pi.txt");
        LOG.info("Writing text output to " + outfile);
        final OutputStream outputstream = fs.create(outfile);
        try {
          final PrintWriter out = new PrintWriter(
              new OutputStreamWriter(outputstream, Charsets.UTF_8), true);
          // write hex text
          print(out, hex.iterator(), "Pi = 0x3.", "%02X", 5, 5);
          out.println("Total number of hexadecimal digits is "
              + 2 * hex.size() + ".");

          // write decimal text
          final Fraction dec = new Fraction(hex);
          final int decDigits = 2 * hex.size(); // TODO: this is conservative.
          print(out, new Iterator<Integer>() {
            private int i = 0;

            public boolean hasNext() {
              return i < decDigits;
            }

            public Integer next() {
              i++;
              return dec.times10();
            }

            public void remove() {
            }
          }, "Pi = 3.", "%d", 10, 5);
          out.println("Total number of decimal digits is " + decDigits + ".");
        } finally {
          outputstream.close();
        }
      }
    }
  }

  /** Print out elements in a nice format. */
  private static <T> void print(PrintWriter out, Iterator<T> iterator,
      String prefix, String format, int elementsPerGroup, int groupsPerLine) {
    final StringBuilder sb = new StringBuilder("\n");
    for (int i = 0; i < prefix.length(); i++)
      sb.append(" ");
    final String spaces = sb.toString();

    out.print("\n" + prefix);
    for (int i = 0; iterator.hasNext(); i++) {
      if (i > 0 && i % elementsPerGroup == 0)
        out.print((i / elementsPerGroup) % groupsPerLine == 0 ? spaces : " ");
      out.print(String.format(format, iterator.next()));
    }
    out.println();
  }

  /** Input split for the {@link BbpInputFormat}. */
  public static class BbpSplit extends InputSplit implements Writable {
    private final static String[] EMPTY = {};

    private long offset;
    private int size;

    /** Public default constructor for the Writable interface. */
    public BbpSplit() {
    }

    private BbpSplit(int i, long offset, int size) {
      LOG.info("Map #" + i + ": workload=" + workload(offset, size)
          + ", offset=" + offset + ", size=" + size);
      this.offset = offset;
      this.size = size;
    }

    private long getOffset() {
      return offset;
    }

    /** {@inheritDoc} */
    public long getLength() {
      return size;
    }

    /** No location is needed. */
    public String[] getLocations() {
      return EMPTY;
    }

    /** {@inheritDoc} */
    public void readFields(DataInput in) throws IOException {
      offset = in.readLong();
      size = in.readInt();
    }

    /** {@inheritDoc} */
    public void write(DataOutput out) throws IOException {
      out.writeLong(offset);
      out.writeInt(size);
    }
  }

  /**
   * Input format for the {@link BbpMapper}.
   * Keys and values represent offsets and sizes, respectively.
   */
  public static class BbpInputFormat
      extends InputFormat<LongWritable, IntWritable> {

    /** {@inheritDoc} */
    public List<InputSplit> getSplits(JobContext context) {
      //get the property values
      final int startDigit = context.getConfiguration().getInt(
          DIGIT_START_PROPERTY, 1);
      final int nDigits = context.getConfiguration().getInt(
          DIGIT_SIZE_PROPERTY, 100);
      final int nMaps = context.getConfiguration().getInt(
          DIGIT_PARTS_PROPERTY, 1);

      //create splits
      final List<InputSplit> splits = new ArrayList<InputSplit>(nMaps);
      final int[] parts = partition(startDigit - 1, nDigits, nMaps);
      for (int i = 0; i < parts.length; ++i) {
        final int k = i < parts.length - 1 ? parts[i+1]: nDigits+startDigit-1;
        splits.add(new BbpSplit(i, parts[i], k - parts[i]));
      }
      return splits;
    }

    /** {@inheritDoc} */
    public RecordReader<LongWritable, IntWritable> createRecordReader(
        InputSplit generic, TaskAttemptContext context) {
      final BbpSplit split = (BbpSplit)generic;

      //return a record reader
      return new RecordReader<LongWritable, IntWritable>() {
        boolean done = false;

        public void initialize(InputSplit split, TaskAttemptContext context) {
        }

        public boolean nextKeyValue() {
          //Each record only contains one key.
          return !done ? done = true : false;
        }

        public LongWritable getCurrentKey() {
          return new LongWritable(split.getOffset());
        }

        public IntWritable getCurrentValue() {
          return new IntWritable((int)split.getLength());
        }

        public float getProgress() {
          return done? 1f: 0f;
        }

        public void close() {
        }
      };
    }
  }

  /** Create and setup a job */
  private static Job createJob(String name, Configuration conf
      ) throws IOException {
    final Job job = Job.getInstance(conf, NAME + "_" + name);
    final Configuration jobconf = job.getConfiguration();
    job.setJarByClass(BaileyBorweinPlouffe.class);

    // setup mapper
    job.setMapperClass(BbpMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // setup reducer
    job.setReducerClass(BbpReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setNumReduceTasks(1);

    // setup input
    job.setInputFormatClass(BbpInputFormat.class);

    // disable task timeout
    jobconf.setLong(MRJobConfig.TASK_TIMEOUT, 0);

    // do not use speculative execution
    jobconf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
    jobconf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
    return job;
  }

  /** Run a map/reduce job to compute Pi. */
  private static void compute(int startDigit, int nDigits, int nMaps,
      String workingDir, Configuration conf, PrintStream out
      ) throws IOException {
    final String name = startDigit + "_" + nDigits;

    //setup wroking directory
    out.println("Working Directory = " + workingDir);
    out.println();
    final FileSystem fs = FileSystem.get(conf);
    final Path dir = fs.makeQualified(new Path(workingDir));
    if (fs.exists(dir)) {
      throw new IOException("Working directory " + dir
          + " already exists.  Please remove it first.");
    } else if (!fs.mkdirs(dir)) {
      throw new IOException("Cannot create working directory " + dir);
    }

    out.println("Start Digit      = " + startDigit);
    out.println("Number of Digits = " + nDigits);
    out.println("Number of Maps   = " + nMaps);

    // setup a job
    final Job job = createJob(name, conf);
    final Path hexfile = new Path(dir, "pi_" + name + ".hex");
    FileOutputFormat.setOutputPath(job, new Path(dir, "out"));

    // setup custom properties
    job.getConfiguration().set(WORKING_DIR_PROPERTY, dir.toString());
    job.getConfiguration().set(HEX_FILE_PROPERTY, hexfile.toString());

    job.getConfiguration().setInt(DIGIT_START_PROPERTY, startDigit);
    job.getConfiguration().setInt(DIGIT_SIZE_PROPERTY, nDigits);
    job.getConfiguration().setInt(DIGIT_PARTS_PROPERTY, nMaps);

    // start a map/reduce job
    out.println("\nStarting Job ...");
    final long startTime = Time.monotonicNow();
    try {
      if (!job.waitForCompletion(true)) {
        out.println("Job failed.");
        System.exit(1);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      final double duration = (Time.monotonicNow() - startTime)/1000.0;
      out.println("Duration is " + duration + " seconds.");
    }
    out.println("Output file: " + hexfile);
  }

  /**
   * Parse arguments and then runs a map/reduce job.
   * @return a non-zero value if there is an error. Otherwise, return 0.
   */
  public int run(String[] args) throws IOException {
    if (args.length != 4) {
      System.err.println("Usage: bbp "
          + " <startDigit> <nDigits> <nMaps> <workingDir>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    final int startDigit = Integer.parseInt(args[0]);
    final int nDigits = Integer.parseInt(args[1]);
    final int nMaps = Integer.parseInt(args[2]);
    final String workingDir = args[3];

    if (startDigit <= 0) {
      throw new IllegalArgumentException("startDigit = " + startDigit+" <= 0");
    } else if (nDigits <= 0) {
      throw new IllegalArgumentException("nDigits = " + nDigits + " <= 0");
    } else if (nDigits % BBP_HEX_DIGITS != 0) {
      throw new IllegalArgumentException("nDigits = " + nDigits
          + " is not a multiple of " + BBP_HEX_DIGITS);
    } else if (nDigits - 1L + startDigit > IMPLEMENTATION_LIMIT + BBP_HEX_DIGITS) {
      throw new UnsupportedOperationException("nDigits - 1 + startDigit = "
          + (nDigits - 1L + startDigit)
          + " > IMPLEMENTATION_LIMIT + BBP_HEX_DIGITS,"
          + ", where IMPLEMENTATION_LIMIT=" + IMPLEMENTATION_LIMIT
          + "and BBP_HEX_DIGITS=" + BBP_HEX_DIGITS);
    } else if (nMaps <= 0) {
      throw new IllegalArgumentException("nMaps = " + nMaps + " <= 0");
    }

    compute(startDigit, nDigits, nMaps, workingDir, getConf(), System.out);
    return 0;
  }

  /** The main method for running it as a stand alone command. */
  public static void main(String[] argv) throws Exception {
    System.exit(ToolRunner.run(null, new BaileyBorweinPlouffe(), argv));
  }

    /////////////////////////////////////////////////////////////////////
   // static fields and methods for Bailey-Borwein-Plouffe algorithm. //
  /////////////////////////////////////////////////////////////////////

  /** Limitation of the program.
   * The program may return incorrect results if the limit is exceeded.
   * The default value is 10^8.
   * The program probably can handle some higher values such as 2^28.
   */
  private static final long IMPLEMENTATION_LIMIT = 100000000;

  private static final long ACCURACY_BIT = 32;
  private static final long BBP_HEX_DIGITS = 4;
  private static final long BBP_MULTIPLIER = 1 << (4 * BBP_HEX_DIGITS);

  /**
   * Compute the exact (d+1)th to (d+{@link #BBP_HEX_DIGITS})th
   * hex digits of pi.
   */
  static long hexDigits(final long d) {
    if (d < 0) {
      throw new IllegalArgumentException("d = " + d + " < 0");
    } else if (d > IMPLEMENTATION_LIMIT) {
      throw new IllegalArgumentException("d = " + d
          + " > IMPLEMENTATION_LIMIT = " + IMPLEMENTATION_LIMIT);
    }

    final double s1 = sum(1, d);
    final double s4 = sum(4, d);
    final double s5 = sum(5, d);
    final double s6 = sum(6, d);

    double pi = s1 + s1;
    if (pi >= 1)
      pi--;
    pi *= 2;
    if (pi >= 1)
      pi--;

    pi -= s4;
    if (pi < 0)
      pi++;
    pi -= s4;
    if (pi < 0)
      pi++;
    pi -= s5;
    if (pi < 0)
      pi++;
    pi -= s6;
    if (pi < 0)
      pi++;

    return (long) (pi * BBP_MULTIPLIER);
  }

  /**
   * Approximate the fraction part of
   * $16^d \sum_{k=0}^\infty \frac{16^{d-k}}{8k+j}$
   * for d > 0 and j = 1, 4, 5, 6.
   */
  private static double sum(final long j, final long d) {
    long k = j == 1 ? 1 : 0;
    double s = 0;
    if (k <= d) {
      s = 1.0 / ((d << 3) | j);
      for (; k < d; k++) {
        final long n = (k << 3) | j;
        s += mod((d - k) << 2, n) * 1.0 / n;
        if (s >= 1)
          s--;
      }
      k++;
    }

    if (k >= 1L << (ACCURACY_BIT - 7))
      return s;

    for (;; k++) {
      final long n = (k << 3) | j;
      final long shift = (k - d) << 2;
      if (ACCURACY_BIT <= shift || 1L << (ACCURACY_BIT - shift) < n) {
        return s;
      }

      s += 1.0 / (n << shift);
      if (s >= 1)
        s--;
    }
  }

  /** Compute $2^e \mod n$ for e > 0, n > 2 */
  static long mod(final long e, final long n) {
    long mask = (e & 0xFFFFFFFF00000000L) == 0 ? 0x00000000FFFFFFFFL
        : 0xFFFFFFFF00000000L;
    mask &= (e & 0xFFFF0000FFFF0000L & mask) == 0 ? 0x0000FFFF0000FFFFL
        : 0xFFFF0000FFFF0000L;
    mask &= (e & 0xFF00FF00FF00FF00L & mask) == 0 ? 0x00FF00FF00FF00FFL
        : 0xFF00FF00FF00FF00L;
    mask &= (e & 0xF0F0F0F0F0F0F0F0L & mask) == 0 ? 0x0F0F0F0F0F0F0F0FL
        : 0xF0F0F0F0F0F0F0F0L;
    mask &= (e & 0xCCCCCCCCCCCCCCCCL & mask) == 0 ? 0x3333333333333333L
        : 0xCCCCCCCCCCCCCCCCL;
    mask &= (e & 0xAAAAAAAAAAAAAAAAL & mask) == 0 ? 0x5555555555555555L
        : 0xAAAAAAAAAAAAAAAAL;

    long r = 2;
    for (mask >>= 1; mask > 0; mask >>= 1) {
      r *= r;
      r %= n;

      if ((e & mask) != 0) {
        r += r;
        if (r >= n)
          r -= n;
      }
    }
    return r;
  }

  /** Represent a number x in hex for 1 > x >= 0 */
  private static class Fraction {
    private final int[] integers; // only use 24-bit

    private int first = 0; // index to the first non-zero integer

    /** Construct a fraction represented by the bytes. */
    Fraction(List<Byte> bytes) {
      integers = new int[(bytes.size() + 2) / 3];
      for (int i = 0; i < bytes.size(); i++) {
        final int b = 0xFF & bytes.get(i);
        integers[integers.length - 1 - i / 3] |= b << ((2 - i % 3) << 3);
      }
      skipZeros();
    }

    /**
     * Compute y = 10*x and then set x to the fraction part of y, where x is the
     * fraction represented by this object.
     * @return integer part of y
     */
    int times10() {
      int carry = 0;
      for (int i = first; i < integers.length; i++) {
        integers[i] <<= 1;
        integers[i] += carry + (integers[i] << 2);
        carry = integers[i] >> 24;
        integers[i] &= 0xFFFFFF;
      }
      skipZeros();
      return carry;
    }

    private void skipZeros() {
      for(; first < integers.length && integers[first] == 0; first++)
        ;
    }
  }

  /**
   * Partition input so that the workload of each part is
   * approximately the same.
   */
  static int[] partition(final int offset, final int size, final int nParts) {
    final int[] parts = new int[nParts];
    final long total = workload(offset, size);
    final int remainder = offset % 4;

    parts[0] = offset;
    for (int i = 1; i < nParts; i++) {
      final long target = offset + i*(total/nParts) + i*(total%nParts)/nParts;

      //search the closest value
      int low = parts[i - 1];
      int high = offset + size;
      for (; high > low + 4;) {
        final int mid = (high + low - 2 * remainder) / 8 * 4 + remainder;
        final long midvalue = workload(mid);
        if (midvalue == target)
          high = low = mid;
        else if (midvalue > target)
          high = mid;
        else
          low = mid;
      }
      parts[i] = high == low? high:
          workload(high)-target > target-workload(low)?
              low: high;
    }
    return parts;
  }

  private static final long MAX_N = 4294967295L; // prevent overflow

  /** Estimate the workload for input size n (in some unit). */
  private static long workload(final long n) {
    if (n < 0) {
      throw new IllegalArgumentException("n = " + n + " < 0");
    } else if (n > MAX_N) {
      throw new IllegalArgumentException("n = " + n + " > MAX_N = " + MAX_N);
    }
    return (n & 1L) == 0L ? (n >> 1) * (n + 1) : n * ((n + 1) >> 1);
  }

  private static long workload(long offset, long size) {
    return workload(offset + size) - workload(offset);
  }
}
