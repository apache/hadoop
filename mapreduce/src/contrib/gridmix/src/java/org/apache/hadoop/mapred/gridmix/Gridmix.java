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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.tools.rumen.ZombieJobProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Driver class for the Gridmix3 benchmark. Gridmix accepts a timestamped
 * stream (trace) of job/task descriptions. For each job in the trace, the
 * client will submit a corresponding, synthetic job to the target cluster at
 * the rate in the original trace. The intent is to provide a benchmark that
 * can be configured and extended to closely match the measured resource
 * profile of actual, production loads.
 */
public class Gridmix extends Configured implements Tool {

  public static final Log LOG = LogFactory.getLog(Gridmix.class);

  /**
   * Output (scratch) directory for submitted jobs. Relative paths are
   * resolved against the path provided as input and absolute paths remain
   * independent of it. The default is &quot;gridmix&quot;.
   */
  public static final String GRIDMIX_OUT_DIR = "gridmix.output.directory";

  /**
   * Number of submitting threads at the client and upper bound for
   * in-memory split data. Submitting threads precompute InputSplits for
   * submitted jobs. This limits the number of splits held in memory waiting
   * for submission and also permits parallel computation of split data.
   */
  public static final String GRIDMIX_SUB_THR = "gridmix.client.submit.threads";

  /**
   * The depth of the queue of job descriptions. Before splits are computed,
   * a queue of pending descriptions is stored in memoory. This parameter
   * limits the depth of that queue.
   */
  public static final String GRIDMIX_QUE_DEP =
    "gridmix.client.pending.queue.depth";

  /**
   * Multiplier to accelerate or decelerate job submission. As a crude means of
   * sizing a job trace to a cluster, the time separating two jobs is
   * multiplied by this factor.
   */
  public static final String GRIDMIX_SUB_MUL = "gridmix.submit.multiplier";

  // Submit data structures
  private JobFactory factory;
  private JobSubmitter submitter;
  private JobMonitor monitor;
  private Statistics statistics;

  // Shutdown hook
  private final Shutdown sdh = new Shutdown();

  /**
   * Write random bytes at the path provided.
   * @see org.apache.hadoop.mapred.gridmix.GenerateData
   */
  protected void writeInputData(long genbytes, Path ioPath)
      throws IOException, InterruptedException {
    final Configuration conf = getConf();
    final GridmixJob genData = new GenerateData(conf, ioPath, genbytes);
    submitter.add(genData);
    LOG.info("Generating " + StringUtils.humanReadableInt(genbytes) +
        " of test data...");
    // TODO add listeners, use for job dependencies
    TimeUnit.SECONDS.sleep(10);
    try {
      genData.getJob().waitForCompletion(false);
    } catch (ClassNotFoundException e) {
      throw new IOException("Internal error", e);
    }
    if (!genData.getJob().isSuccessful()) {
      throw new IOException("Data generation failed!");
    }
    LOG.info("Done.");
  }

  protected InputStream createInputStream(String in) throws IOException {
    if ("-".equals(in)) {
      return System.in;
    }
    final Path pin = new Path(in);
    return pin.getFileSystem(getConf()).open(pin);
  }

  /**
   * Create each component in the pipeline and start it.
   * @param conf Configuration data, no keys specific to this context
   * @param traceIn Either a Path to the trace data or &quot;-&quot; for
   *                stdin
   * @param ioPath Path from which input data is read
   * @param scratchDir Path into which job output is written
   * @param startFlag Semaphore for starting job trace pipeline
   */
  private void startThreads(Configuration conf, String traceIn, Path ioPath,
      Path scratchDir, CountDownLatch startFlag) throws IOException {
    try {
      GridmixJobSubmissionPolicy policy = GridmixJobSubmissionPolicy.getPolicy(
        conf, GridmixJobSubmissionPolicy.STRESS);
      LOG.info(" Submission policy is " + policy.name());
      statistics = new Statistics(conf, policy.getPollingInterval(), startFlag);
      monitor = createJobMonitor(statistics);
      int noOfSubmitterThreads = policy.name().equals(
        GridmixJobSubmissionPolicy.SERIAL.name()) ? 1 :
        Runtime.getRuntime().availableProcessors() + 1;

      submitter = createJobSubmitter(
        monitor, conf.getInt(
          GRIDMIX_SUB_THR, noOfSubmitterThreads),
        conf.getInt(GRIDMIX_QUE_DEP, 5), new FilePool(conf, ioPath));
      factory = createJobFactory(
        submitter, traceIn, scratchDir, conf, startFlag);
      if (policy.name().equals(GridmixJobSubmissionPolicy.SERIAL.name())) {
        statistics.addJobStatsListeners(factory);
      } else {
        statistics.addClusterStatsObservers(factory);
      }

      monitor.start();
      submitter.start();
    }catch(Exception e) {
      LOG.error(" Exception at start " ,e);
      throw new IOException(e);
    }
   }

  protected JobMonitor createJobMonitor(Statistics stats) throws IOException {
    return new JobMonitor(stats);
  }

  protected JobSubmitter createJobSubmitter(JobMonitor monitor, int threads,
      int queueDepth, FilePool pool) throws IOException {
    return new JobSubmitter(monitor, threads, queueDepth, pool);
  }

  protected JobFactory createJobFactory(JobSubmitter submitter, String traceIn,
      Path scratchDir, Configuration conf, CountDownLatch startFlag)
      throws IOException {
     return GridmixJobSubmissionPolicy.getPolicy(
       conf, GridmixJobSubmissionPolicy.STRESS).createJobFactory(
       submitter, new ZombieJobProducer(
         createInputStream(
           traceIn), null), scratchDir, conf, startFlag);  }

  public int run(String[] argv) throws IOException, InterruptedException {
    if (argv.length < 2) {
      printUsage(System.err);
      return 1;
    }
    long genbytes = 0;
    String traceIn = null;
    Path ioPath = null;
    try {
      int i = 0;
      genbytes = "-generate".equals(argv[i++])
        ? StringUtils.TraditionalBinaryPrefix.string2long(argv[i++])
        : --i;
      ioPath = new Path(argv[i++]);
      traceIn = argv[i++];
      if (i != argv.length) {
        printUsage(System.err);
        return 1;
      }
    } catch (Exception e) {
      printUsage(System.err);
      return 1;
    }
    InputStream trace = null;
    try {
      final Configuration conf = getConf();
      Path scratchDir = new Path(ioPath, conf.get(GRIDMIX_OUT_DIR, "gridmix"));
      // add shutdown hook for SIGINT, etc.
      Runtime.getRuntime().addShutdownHook(sdh);
      CountDownLatch startFlag = new CountDownLatch(1);
      try {
        // Create, start job submission threads
        startThreads(conf, traceIn, ioPath, scratchDir, startFlag);
        // Write input data if specified
        if (genbytes > 0) {
          writeInputData(genbytes, ioPath);
        }
        // scan input dir contents
        submitter.refreshFilePool();
        factory.start();
        statistics.start();
      } catch (Throwable e) {
        LOG.error("Startup failed", e);
        if (factory != null) factory.abort(); // abort pipeline
      } finally {
        // signal for factory to start; sets start time
        startFlag.countDown();
      }
      if (factory != null) {
        // wait for input exhaustion
        factory.join(Long.MAX_VALUE);
        final Throwable badTraceException = factory.error();
        if (null != badTraceException) {
          LOG.error("Error in trace", badTraceException);
          throw new IOException("Error in trace", badTraceException);
        }
        // wait for pending tasks to be submitted
        submitter.shutdown();
        submitter.join(Long.MAX_VALUE);
        // wait for running tasks to complete
        monitor.shutdown();
        monitor.join(Long.MAX_VALUE);

        statistics.shutdown();
        statistics.join(Long.MAX_VALUE);

      }
    } finally {
      IOUtils.cleanup(LOG, trace);
    }
    return 0;
  }

  /**
   * Handles orderly shutdown by requesting that each component in the
   * pipeline abort its progress, waiting for each to exit and killing
   * any jobs still running on the cluster.
   */
  class Shutdown extends Thread {

    static final long FAC_SLEEP = 1000;
    static final long SUB_SLEEP = 4000;
    static final long MON_SLEEP = 15000;

    private void killComponent(Component<?> component, long maxwait) {
      if (component == null) {
        return;
      }
      component.abort();
      try {
        component.join(maxwait);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted waiting for " + component);
      }

    }

    @Override
    public void run() {
      LOG.info("Exiting...");
      try {
        killComponent(factory, FAC_SLEEP);   // read no more tasks
        killComponent(submitter, SUB_SLEEP); // submit no more tasks
        killComponent(monitor, MON_SLEEP);   // process remaining jobs here
        killComponent(statistics,MON_SLEEP);
      } finally {
        if (monitor == null) {
          return;
        }
        List<Job> remainingJobs = monitor.getRemainingJobs();
        if (remainingJobs.isEmpty()) {
          return;
        }
        LOG.info("Killing running jobs...");
        for (Job job : remainingJobs) {
          try {
            if (!job.isComplete()) {
              job.killJob();
              LOG.info("Killed " + job.getJobName() + " (" + job.getJobID() + ")");
            } else {
              if (job.isSuccessful()) {
                monitor.onSuccess(job);
              } else {
                monitor.onFailure(job);
              }
            }
          } catch (IOException e) {
            LOG.warn("Failure killing " + job.getJobName(), e);
          } catch (Exception e) {
            LOG.error("Unexcpected exception", e);
          }
        }
        LOG.info("Done.");
      }
    }

  }

  public static void main(String[] argv) throws Exception {
    int res = -1;
    try {
      res = ToolRunner.run(new Configuration(), new Gridmix(), argv);
    } finally {
      System.exit(res);
    }
  }

  protected void printUsage(PrintStream out) {
    ToolRunner.printGenericCommandUsage(out);
    out.println("Usage: gridmix [-generate <MiB>] <iopath> <trace>");
    out.println("  e.g. gridmix -generate 100m foo -");
    out.println("Configuration parameters:");
    out.printf("       %-40s : Output directory\n", GRIDMIX_OUT_DIR);
    out.printf("       %-40s : Submitting threads\n", GRIDMIX_SUB_THR);
    out.printf("       %-40s : Queued job desc\n", GRIDMIX_QUE_DEP);
    out.printf("       %-40s : Key fraction of rec\n",
        AvgRecordFactory.GRIDMIX_KEY_FRC);
  }

  /**
   * Components in the pipeline must support the following operations for
   * orderly startup and shutdown.
   */
  interface Component<T> {

    /**
     * Accept an item into this component from an upstream component. If
     * shutdown or abort have been called, this may fail, depending on the
     * semantics for the component.
     */
    void add(T item) throws InterruptedException;

    /**
     * Attempt to start the service.
     */
    void start();

    /**
     * Wait until the service completes. It is assumed that either a
     * {@link #shutdown} or {@link #abort} has been requested.
     */
    void join(long millis) throws InterruptedException;

    /**
     * Shut down gracefully, finishing all pending work. Reject new requests.
     */
    void shutdown();

    /**
     * Shut down immediately, aborting any work in progress and discarding
     * all pending work. It is legal to store pending work for another
     * thread to process.
     */
    void abort();
  }

}
