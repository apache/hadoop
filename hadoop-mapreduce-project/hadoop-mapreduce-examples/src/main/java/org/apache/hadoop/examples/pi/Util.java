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
package org.apache.hadoop.examples.pi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.examples.pi.DistSum.Machine;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

/** Utility methods */
public class Util {
  /** Output stream */
  public static final PrintStream out = System.out; 
  /** Error stream */
  public static final PrintStream err = System.out; 

  /** Timer */
  public static class Timer {
    private final boolean isAccumulative;
    private final long start = System.currentTimeMillis();
    private long previous = start;
  
    /** Timer constructor
     * @param isAccumulative  Is accumulating the time duration?
     */
    public Timer(boolean isAccumulative) {
      this.isAccumulative = isAccumulative;
      final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      final StackTraceElement e = stack[stack.length - 1];
      out.println(e + " started at " + new Date(start));
    }
  
    /** Same as tick(null). */
    public long tick() {return tick(null);}

    /**
     * Tick
     * @param s Output message.  No output if it is null.
     * @return delta
     */
    public synchronized long tick(String s) {
      final long t = System.currentTimeMillis();
      final long delta = t - (isAccumulative? start: previous);
      if (s != null) {
        out.format("%15dms (=%-15s: %s%n", delta, millis2String(delta) + ")", s);
        out.flush();
      }
      previous = t;
      return delta;
    }
  }
    
  /** Covert milliseconds to a String. */
  public static String millis2String(long n) {
    if (n < 0)
      return "-" + millis2String(-n);
    else if (n < 1000)
      return n + "ms";

    final StringBuilder b = new StringBuilder();
    final int millis = (int)(n % 1000L);
    if (millis != 0)
      b.append(String.format(".%03d", millis)); 
    if ((n /= 1000) < 60)
      return b.insert(0, n).append("s").toString();

    b.insert(0, String.format(":%02d", (int)(n % 60L)));
    if ((n /= 60) < 60)
      return b.insert(0, n).toString();

    b.insert(0, String.format(":%02d", (int)(n % 60L)));
    if ((n /= 60) < 24)
      return b.insert(0, n).toString();

    b.insert(0, n % 24L);
    final int days = (int)((n /= 24) % 365L);
    b.insert(0, days == 1? " day ": " days ").insert(0, days);
    if ((n /= 365L) > 0)
      b.insert(0, n == 1? " year ": " years ").insert(0, n);

    return b.toString();
  }

  /** Covert a String to a long.  
   * This support comma separated number format.
   */
  public static long string2long(String s) {
    return Long.parseLong(s.trim().replace(",", ""));
  }

  /** Covert a long to a String in comma separated number format. */  
  public static String long2string(long n) {
    if (n < 0)
      return "-" + long2string(-n);
    
    final StringBuilder b = new StringBuilder();
    for(; n >= 1000; n = n/1000)
      b.insert(0, String.format(",%03d", n % 1000));
    return n + b.toString();    
  }

  /** Parse a variable. */  
  public static long parseLongVariable(final String name, final String s) {
    return string2long(parseStringVariable(name, s));
  }

  /** Parse a variable. */  
  public static String parseStringVariable(final String name, final String s) {
    if (!s.startsWith(name + '='))
      throw new IllegalArgumentException("!s.startsWith(name + '='), name="
          + name + ", s=" + s);
    return s.substring(name.length() + 1);
  }

  /** Execute the callables by a number of threads */
  public static <T, E extends Callable<T>> void execute(int nThreads, List<E> callables
      ) throws InterruptedException, ExecutionException {
    final ExecutorService executor = HadoopExecutors.newFixedThreadPool(
        nThreads);
    final List<Future<T>> futures = executor.invokeAll(callables);
    for(Future<T> f : futures)
      f.get();
  }

  /** Print usage messages */
  public static int printUsage(String[] args, String usage) {
    err.println("args = " + Arrays.asList(args));
    err.println();
    err.println("Usage: java " + usage);
    err.println();
    ToolRunner.printGenericCommandUsage(err);
    return -1;
  }

  /** Combine a list of items. */
  public static <T extends Combinable<T>> List<T> combine(Collection<T> items) {
    final List<T> sorted = new ArrayList<T>(items);
    if (sorted.size() <= 1)
      return sorted;

    Collections.sort(sorted);
    final List<T> combined = new ArrayList<T>(items.size());
    T prev = sorted.get(0);
    for(int i = 1; i < sorted.size(); i++) {
      final T curr = sorted.get(i);
      final T c = curr.combine(prev);

      if (c != null)
        prev = c;
      else {
        combined.add(prev);
        prev = curr;
      }
    }
    combined.add(prev);
    return combined;
  }

  /** Check local directory. */
  public static void checkDirectory(File dir) {
    if (!dir.exists())
      if (!dir.mkdirs())
        throw new IllegalArgumentException("!dir.mkdirs(), dir=" + dir);
    if (!dir.isDirectory())
      throw new IllegalArgumentException("dir (=" + dir + ") is not a directory.");
  }

  /** Create a writer of a local file. */
  public static PrintWriter createWriter(File dir, String prefix) throws IOException {
    checkDirectory(dir);
    
    SimpleDateFormat dateFormat = new SimpleDateFormat("-yyyyMMdd-HHmmssSSS");
    for(;;) {
      final File f = new File(dir,
          prefix + dateFormat.format(new Date(System.currentTimeMillis())) + ".txt");
      if (!f.exists())
        return new PrintWriter(new OutputStreamWriter(new FileOutputStream(f), Charsets.UTF_8));

      try {Thread.sleep(10);} catch (InterruptedException e) {}
    }
  }

  /** Print a "bits skipped" message. */
  public static void printBitSkipped(final long b) {
    out.println();
    out.println("b = " + long2string(b)
        + " (" + (b < 2? "bit": "bits") + " skipped)");
  }

  /** Convert a pi value to a String. */
  public static String pi2string(final double pi, final long terms) {
    final long value = (long)(pi * (1L << DOUBLE_PRECISION));
    final int acc_bit = accuracy(terms, false);
    final int acc_hex = acc_bit/4;
    final int shift = DOUBLE_PRECISION - acc_bit;
    return String.format("%0" + acc_hex + "X %0" + (13-acc_hex) + "X (%d hex digits)",
        value >> shift, value & ((1 << shift) - 1), acc_hex);
  }

  static final int DOUBLE_PRECISION = 52; //mantissa size
  static final int MACHEPS_EXPONENT = DOUBLE_PRECISION + 1;
  /** Estimate accuracy. */
  public static int accuracy(final long terms, boolean print) {
    final double error = terms <= 0? 2: (Math.log(terms) / Math.log(2)) / 2;
    final int bits = MACHEPS_EXPONENT - (int)Math.ceil(error);
    if (print)
      out.println("accuracy: bits=" + bits + ", terms=" + long2string(terms) + ", error exponent=" + error);
    return bits - bits%4;
  }

  private static final String JOB_SEPARATION_PROPERTY = "pi.job.separation.seconds";
  private static final Semaphore JOB_SEMAPHORE = new Semaphore(1);

  /** Run a job. */
  static void runJob(String name, Job job, Machine machine, String startmessage, Util.Timer timer) {
    JOB_SEMAPHORE.acquireUninterruptibly();
    Long starttime = null;
    try {
      try {
        starttime = timer.tick("starting " + name + " ...\n  " + startmessage);

        //initialize and submit a job
        machine.init(job);
        job.submit();
  
        // Separate jobs
        final long sleeptime = 1000L * job.getConfiguration().getInt(JOB_SEPARATION_PROPERTY, 10);
        if (sleeptime > 0) {
          Util.out.println(name + "> sleep(" + Util.millis2String(sleeptime) + ")");
          Thread.sleep(sleeptime);
        }
      } finally {
        JOB_SEMAPHORE.release();
      }
  
      if (!job.waitForCompletion(false))
        throw new RuntimeException(name + " failed.");
    } catch(Exception e) {
      throw e instanceof RuntimeException? (RuntimeException)e: new RuntimeException(e);
    } finally {
      if (starttime != null)
        timer.tick(name + "> timetaken=" + Util.millis2String(timer.tick() - starttime));
    }
  }

  /** Read job outputs */
  static List<TaskResult> readJobOutputs(FileSystem fs, Path outdir) throws IOException {
    final List<TaskResult> results = new ArrayList<TaskResult>();
    for(FileStatus status : fs.listStatus(outdir)) {
      if (status.getPath().getName().startsWith("part-")) {
        final BufferedReader in = new BufferedReader(
            new InputStreamReader(fs.open(status.getPath()), Charsets.UTF_8));
        try {
          for(String line; (line = in.readLine()) != null; )
            results.add(TaskResult.valueOf(line));
        }
        finally {
          in.close();
        }
      }
    }
    if (results.isEmpty())
      throw new IOException("Output not found");
    return results;
  }
  
  /** Write results */
  static void writeResults(String name, List<TaskResult> results, FileSystem fs, String dir) throws IOException {
    final Path outfile = new Path(dir, name + ".txt");
    Util.out.println(name + "> writing results to " + outfile);
    final PrintWriter out = new PrintWriter(new OutputStreamWriter(fs.create(outfile), Charsets.UTF_8), true);
    try {
      for(TaskResult r : results)
        out.println(r);
    }
    finally {
      out.close();
    }
  }

  /** Create a directory. */
  static boolean createNonexistingDirectory(FileSystem fs, Path dir) throws IOException {
    if (fs.exists(dir)) {
      Util.err.println("dir (= " + dir + ") already exists.");
      return false;
    } else if (!fs.mkdirs(dir)) {
      throw new IOException("Cannot create working directory " + dir);
    }
    fs.setPermission(dir, new FsPermission((short)0777));
    return true;
  }
}