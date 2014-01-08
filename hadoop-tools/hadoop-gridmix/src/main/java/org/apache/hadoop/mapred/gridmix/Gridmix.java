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
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.gridmix.GenerateData.DataStatistics;
import org.apache.hadoop.mapred.gridmix.Statistics.JobStats;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
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

  /**
   * Class used to resolve users in the trace to the list of target users
   * on the cluster.
   */
  public static final String GRIDMIX_USR_RSV = "gridmix.user.resolve.class";

  /**
   * The configuration key which determines the duration for which the 
   * job-monitor sleeps while polling for job status.
   * This value should be specified in milliseconds.
   */
  public static final String GRIDMIX_JOBMONITOR_SLEEPTIME_MILLIS = 
    "gridmix.job-monitor.sleep-time-ms";
  
  /**
   * Default value for {@link #GRIDMIX_JOBMONITOR_SLEEPTIME_MILLIS}.
   */
  public static final int GRIDMIX_JOBMONITOR_SLEEPTIME_MILLIS_DEFAULT = 500;
  
  /**
   * The configuration key which determines the total number of job-status
   * monitoring threads.
   */
  public static final String GRIDMIX_JOBMONITOR_THREADS = 
    "gridmix.job-monitor.thread-count";
  
  /**
   * Default value for {@link #GRIDMIX_JOBMONITOR_THREADS}.
   */
  public static final int GRIDMIX_JOBMONITOR_THREADS_DEFAULT = 1;
  
  /**
   * Configuration property set in simulated job's configuration whose value is
   * set to the corresponding original job's name. This is not configurable by
   * gridmix user.
   */
  public static final String ORIGINAL_JOB_NAME =
      "gridmix.job.original-job-name";
  /**
   * Configuration property set in simulated job's configuration whose value is
   * set to the corresponding original job's id. This is not configurable by
   * gridmix user.
   */
  public static final String ORIGINAL_JOB_ID = "gridmix.job.original-job-id";

  private DistributedCacheEmulator distCacheEmulator;

  // Submit data structures
  private JobFactory factory;
  private JobSubmitter submitter;
  private JobMonitor monitor;
  private Statistics statistics;
  private Summarizer summarizer;

  // Shutdown hook
  private final Shutdown sdh = new Shutdown();

  /** Error while parsing/analyzing the arguments to Gridmix */
  static final int ARGS_ERROR = 1;
  /** Error while trying to start/setup the Gridmix run */
  static final int STARTUP_FAILED_ERROR = 2;
  /**
   * If at least 1 distributed cache file is missing in the expected
   * distributed cache dir, Gridmix cannot proceed with emulation of
   * distributed cache load.
   */
  static final int MISSING_DIST_CACHE_FILES_ERROR = 3;


  Gridmix(String[] args) {
    summarizer = new Summarizer(args);
  }
  
  public Gridmix() {
    summarizer = new Summarizer();
  }
  
  // Get the input data directory for Gridmix. Input directory is 
  // <io-path>/input
  static Path getGridmixInputDataPath(Path ioPath) {
    return new Path(ioPath, "input");
  }
  
  /**
   * Write random bytes at the path &lt;inputDir&gt; if needed.
   * @see org.apache.hadoop.mapred.gridmix.GenerateData
   * @return exit status
   */
  protected int writeInputData(long genbytes, Path inputDir)
      throws IOException, InterruptedException {
    if (genbytes > 0) {
      final Configuration conf = getConf();

      if (inputDir.getFileSystem(conf).exists(inputDir)) {
        LOG.error("Gridmix input data directory " + inputDir
                  + " already exists when -generate option is used.\n");
        return STARTUP_FAILED_ERROR;
      }

      // configure the compression ratio if needed
      CompressionEmulationUtil.setupDataGeneratorConfig(conf);
    
      final GenerateData genData = new GenerateData(conf, inputDir, genbytes);
      LOG.info("Generating " + StringUtils.humanReadableInt(genbytes) +
               " of test data...");
      launchGridmixJob(genData);
    
      FsShell shell = new FsShell(conf);
      try {
        LOG.info("Changing the permissions for inputPath " + inputDir.toString());
        shell.run(new String[] {"-chmod","-R","777", inputDir.toString()});
      } catch (Exception e) {
        LOG.error("Couldnt change the file permissions " , e);
        throw new IOException(e);
      }

      LOG.info("Input data generation successful.");
    }

    return 0;
  }

  /**
   * Write random bytes in the distributed cache files that will be used by all
   * simulated jobs of current gridmix run, if files are to be generated.
   * Do this as part of the MapReduce job {@link GenerateDistCacheData#JOB_NAME}
   * @see org.apache.hadoop.mapred.gridmix.GenerateDistCacheData
   */
  protected void writeDistCacheData(Configuration conf)
      throws IOException, InterruptedException {
    int fileCount =
        conf.getInt(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_COUNT, -1);
    if (fileCount > 0) {// generate distributed cache files
      final GridmixJob genDistCacheData = new GenerateDistCacheData(conf);
      LOG.info("Generating distributed cache data of size " + conf.getLong(
          GenerateDistCacheData.GRIDMIX_DISTCACHE_BYTE_COUNT, -1));
      launchGridmixJob(genDistCacheData);
    }
  }

  // Launch Input/DistCache Data Generation job and wait for completion
  void launchGridmixJob(GridmixJob job)
      throws IOException, InterruptedException {
    submitter.add(job);

    // TODO add listeners, use for job dependencies
    try {
      while (!job.isSubmitted()) {
        try {
            Thread.sleep(100); // sleep
          } catch (InterruptedException ie) {}
      }
      // wait for completion
      job.getJob().waitForCompletion(false);
    } catch (ClassNotFoundException e) {
      throw new IOException("Internal error", e);
    }
    if (!job.getJob().isSuccessful()) {
      throw new IOException(job.getJob().getJobName() + " job failed!");
    }
  }

  /**
   * Create an appropriate {@code JobStoryProducer} object for the
   * given trace.
   * 
   * @param traceIn the path to the trace file. The special path
   * &quot;-&quot; denotes the standard input stream.
   *
   * @param conf the configuration to be used.
   *
   * @throws IOException if there was an error.
   */
  protected JobStoryProducer createJobStoryProducer(String traceIn,
      Configuration conf) throws IOException {
    if ("-".equals(traceIn)) {
      return new ZombieJobProducer(System.in, null);
    }
    return new ZombieJobProducer(new Path(traceIn), null, conf);
  }

  // get the gridmix job submission policy
  protected static GridmixJobSubmissionPolicy getJobSubmissionPolicy(
                                                Configuration conf) {
    return GridmixJobSubmissionPolicy.getPolicy(conf, 
                                        GridmixJobSubmissionPolicy.STRESS);
  }
  
  /**
   * Create each component in the pipeline and start it.
   * @param conf Configuration data, no keys specific to this context
   * @param traceIn Either a Path to the trace data or &quot;-&quot; for
   *                stdin
   * @param ioPath &lt;ioPath&gt;/input/ is the dir from which input data is
   *               read and &lt;ioPath&gt;/distributedCache/ is the gridmix
   *               distributed cache directory.
   * @param scratchDir Path into which job output is written
   * @param startFlag Semaphore for starting job trace pipeline
   */
  private void startThreads(Configuration conf, String traceIn, Path ioPath,
      Path scratchDir, CountDownLatch startFlag, UserResolver userResolver)
      throws IOException {
    try {
      Path inputDir = getGridmixInputDataPath(ioPath);
      GridmixJobSubmissionPolicy policy = getJobSubmissionPolicy(conf);
      LOG.info(" Submission policy is " + policy.name());
      statistics = new Statistics(conf, policy.getPollingInterval(), startFlag);
      monitor = createJobMonitor(statistics, conf);
      int noOfSubmitterThreads = 
        (policy == GridmixJobSubmissionPolicy.SERIAL) 
        ? 1
        : Runtime.getRuntime().availableProcessors() + 1;

      int numThreads = conf.getInt(GRIDMIX_SUB_THR, noOfSubmitterThreads);
      int queueDep = conf.getInt(GRIDMIX_QUE_DEP, 5);
      submitter = createJobSubmitter(monitor, numThreads, queueDep,
                                     new FilePool(conf, inputDir), userResolver, 
                                     statistics);
      distCacheEmulator = new DistributedCacheEmulator(conf, ioPath);

      factory = createJobFactory(submitter, traceIn, scratchDir, conf, 
                                 startFlag, userResolver);
      factory.jobCreator.setDistCacheEmulator(distCacheEmulator);

      if (policy == GridmixJobSubmissionPolicy.SERIAL) {
        statistics.addJobStatsListeners(factory);
      } else {
        statistics.addClusterStatsObservers(factory);
      }

      // add the gridmix run summarizer to the statistics
      statistics.addJobStatsListeners(summarizer.getExecutionSummarizer());
      statistics.addClusterStatsObservers(summarizer.getClusterSummarizer());
      
      monitor.start();
      submitter.start();
    }catch(Exception e) {
      LOG.error(" Exception at start " ,e);
      throw new IOException(e);
    }
   }

  protected JobMonitor createJobMonitor(Statistics stats, Configuration conf) 
  throws IOException {
    int delay = conf.getInt(GRIDMIX_JOBMONITOR_SLEEPTIME_MILLIS, 
                            GRIDMIX_JOBMONITOR_SLEEPTIME_MILLIS_DEFAULT);
    int numThreads = conf.getInt(GRIDMIX_JOBMONITOR_THREADS, 
                                 GRIDMIX_JOBMONITOR_THREADS_DEFAULT);
    return new JobMonitor(delay, TimeUnit.MILLISECONDS, stats, numThreads);
  }

  protected JobSubmitter createJobSubmitter(JobMonitor monitor, int threads,
      int queueDepth, FilePool pool, UserResolver resolver, 
      Statistics statistics) throws IOException {
    return new JobSubmitter(monitor, threads, queueDepth, pool, statistics);
  }

  protected JobFactory createJobFactory(JobSubmitter submitter, String traceIn,
      Path scratchDir, Configuration conf, CountDownLatch startFlag, 
      UserResolver resolver)
      throws IOException {
     return GridmixJobSubmissionPolicy.getPolicy(
       conf, GridmixJobSubmissionPolicy.STRESS).createJobFactory(
       submitter, createJobStoryProducer(traceIn, conf), scratchDir, conf,
       startFlag, resolver);
  }

  private static UserResolver userResolver;

  public UserResolver getCurrentUserResolver() {
    return userResolver;
  }
  
  public int run(final String[] argv) throws IOException, InterruptedException {
    int val = -1;
    final Configuration conf = getConf();
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();

    val = ugi.doAs(new PrivilegedExceptionAction<Integer>() {
      public Integer run() throws Exception {
        return runJob(conf, argv);
      }
    });
    
    // print the gridmix summary if the run was successful
    if (val == 0) {
        // print the run summary
        System.out.print("\n\n");
        System.out.println(summarizer.toString());
    }
    
    return val; 
  }

  private int runJob(Configuration conf, String[] argv)
    throws IOException, InterruptedException {
    if (argv.length < 2) {
      LOG.error("Too few arguments to Gridmix.\n");
      printUsage(System.err);
      return ARGS_ERROR;
    }

    long genbytes = -1L;
    String traceIn = null;
    Path ioPath = null;
    URI userRsrc = null;
    try {
      userResolver = ReflectionUtils.newInstance(conf.getClass(GRIDMIX_USR_RSV, 
                       SubmitterUserResolver.class, UserResolver.class), conf);

      for (int i = 0; i < argv.length - 2; ++i) {
        if ("-generate".equals(argv[i])) {
          genbytes = StringUtils.TraditionalBinaryPrefix.string2long(argv[++i]);
          if (genbytes <= 0) {
            LOG.error("size of input data to be generated specified using "
                      + "-generate option should be nonnegative.\n");
            return ARGS_ERROR;
          }
        } else if ("-users".equals(argv[i])) {
          userRsrc = new URI(argv[++i]);
        } else {
          LOG.error("Unknown option " + argv[i] + " specified.\n");
          printUsage(System.err);
          return ARGS_ERROR;
        }
      }

      if (userResolver.needsTargetUsersList()) {
        if (userRsrc != null) {
          if (!userResolver.setTargetUsers(userRsrc, conf)) {
            LOG.warn("Ignoring the user resource '" + userRsrc + "'.");
          }
        } else {
          LOG.error(userResolver.getClass()
              + " needs target user list. Use -users option.\n");
          printUsage(System.err);
          return ARGS_ERROR;
        }
      } else if (userRsrc != null) {
        LOG.warn("Ignoring the user resource '" + userRsrc + "'.");
      }

      ioPath = new Path(argv[argv.length - 2]);
      traceIn = argv[argv.length - 1];
    } catch (Exception e) {
      LOG.error(e.toString() + "\n");
      if (LOG.isDebugEnabled()) {
        e.printStackTrace();
      }

      printUsage(System.err);
      return ARGS_ERROR;
    }

    // Create <ioPath> with 777 permissions
    final FileSystem inputFs = ioPath.getFileSystem(conf);
    ioPath = ioPath.makeQualified(inputFs);
    boolean succeeded = false;
    try {
      succeeded = FileSystem.mkdirs(inputFs, ioPath,
                                    new FsPermission((short)0777));
    } catch(IOException e) {
      // No need to emit this exception message
    } finally {
      if (!succeeded) {
        LOG.error("Failed creation of <ioPath> directory " + ioPath + "\n");
        return STARTUP_FAILED_ERROR;
      }
    }

    return start(conf, traceIn, ioPath, genbytes, userResolver);
  }

  /**
   * 
   * @param conf gridmix configuration
   * @param traceIn trace file path(if it is '-', then trace comes from the
   *                stream stdin)
   * @param ioPath Working directory for gridmix. GenerateData job
   *               will generate data in the directory &lt;ioPath&gt;/input/ and
   *               distributed cache data is generated in the directory
   *               &lt;ioPath&gt;/distributedCache/, if -generate option is
   *               specified.
   * @param genbytes size of input data to be generated under the directory
   *                 &lt;ioPath&gt;/input/
   * @param userResolver gridmix user resolver
   * @return exit code
   * @throws IOException
   * @throws InterruptedException
   */
  int start(Configuration conf, String traceIn, Path ioPath, long genbytes,
      UserResolver userResolver)
      throws IOException, InterruptedException {
    DataStatistics stats = null;
    InputStream trace = null;
    int exitCode = 0;

    try {
      Path scratchDir = new Path(ioPath, conf.get(GRIDMIX_OUT_DIR, "gridmix"));

      // add shutdown hook for SIGINT, etc.
      Runtime.getRuntime().addShutdownHook(sdh);
      CountDownLatch startFlag = new CountDownLatch(1);
      try {
        // Create, start job submission threads
        startThreads(conf, traceIn, ioPath, scratchDir, startFlag,
                     userResolver);
        
        Path inputDir = getGridmixInputDataPath(ioPath);
        
        // Write input data if specified
        exitCode = writeInputData(genbytes, inputDir);
        if (exitCode != 0) {
          return exitCode;
        }

        // publish the data statistics
        stats = GenerateData.publishDataStatistics(inputDir, genbytes, conf);
        
        // scan input dir contents
        submitter.refreshFilePool();

        boolean shouldGenerate = (genbytes > 0);
        // set up the needed things for emulation of various loads
        exitCode = setupEmulation(conf, traceIn, scratchDir, ioPath,
                                  shouldGenerate);
        if (exitCode != 0) {
          return exitCode;
        }

        // start the summarizer
        summarizer.start(conf);
        
        factory.start();
        statistics.start();
      } catch (Throwable e) {
        LOG.error("Startup failed. " + e.toString() + "\n");
        if (LOG.isDebugEnabled()) {
          e.printStackTrace();
        }
        if (factory != null) factory.abort(); // abort pipeline
        exitCode = STARTUP_FAILED_ERROR;
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
      if (factory != null) {
        summarizer.finalize(factory, traceIn, genbytes, userResolver, stats, 
                            conf);
      }
      IOUtils.cleanup(LOG, trace);
    }
    return exitCode;
  }

  /**
   * Create gridmix output directory. Setup things for emulation of
   * various loads, if needed.
   * @param conf gridmix configuration
   * @param traceIn trace file path(if it is '-', then trace comes from the
   *                stream stdin)
   * @param scratchDir gridmix output directory
   * @param ioPath Working directory for gridmix.
   * @param generate true if -generate option was specified
   * @return exit code
   * @throws IOException
   * @throws InterruptedException 
   */
  private int setupEmulation(Configuration conf, String traceIn,
      Path scratchDir, Path ioPath, boolean generate)
      throws IOException, InterruptedException {
    // create scratch directory(output directory of gridmix)
    final FileSystem scratchFs = scratchDir.getFileSystem(conf);
    FileSystem.mkdirs(scratchFs, scratchDir, new FsPermission((short) 0777));

    // Setup things needed for emulation of distributed cache load
    return setupDistCacheEmulation(conf, traceIn, ioPath, generate);
    // Setup emulation of other loads like CPU load, Memory load
  }

  /**
   * Setup gridmix for emulation of distributed cache load. This includes
   * generation of distributed cache files, if needed.
   * @param conf gridmix configuration
   * @param traceIn trace file path(if it is '-', then trace comes from the
   *                stream stdin)
   * @param ioPath &lt;ioPath&gt;/input/ is the dir where input data (a) exists
   *               or (b) is generated. &lt;ioPath&gt;/distributedCache/ is the
   *               folder where distributed cache data (a) exists or (b) is to be
   *               generated by gridmix.
   * @param generate true if -generate option was specified
   * @return exit code
   * @throws IOException
   * @throws InterruptedException
   */
  private int setupDistCacheEmulation(Configuration conf, String traceIn,
      Path ioPath, boolean generate) throws IOException, InterruptedException {
    distCacheEmulator.init(traceIn, factory.jobCreator, generate);
    int exitCode = 0;
    if (distCacheEmulator.shouldGenerateDistCacheData() ||
        distCacheEmulator.shouldEmulateDistCacheLoad()) {

      JobStoryProducer jsp = createJobStoryProducer(traceIn, conf);
      exitCode = distCacheEmulator.setupGenerateDistCacheData(jsp);
      if (exitCode == 0) {
        // If there are files to be generated, run a MapReduce job to generate
        // these distributed cache files of all the simulated jobs of this trace.
        writeDistCacheData(conf);
      }
    }
    return exitCode;
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
        List<JobStats> remainingJobs = monitor.getRemainingJobs();
        if (remainingJobs.isEmpty()) {
          return;
        }
        LOG.info("Killing running jobs...");
        for (JobStats stats : remainingJobs) {
          Job job = stats.getJob();
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
      res = ToolRunner.run(new Configuration(), new Gridmix(argv), argv);
    } finally {
      ExitUtil.terminate(res);
    }
  }

  private String getEnumValues(Enum<?>[] e) {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (Enum<?> v : e) {
      sb.append(sep);
      sb.append(v.name());
      sep = "|";
    }
    return sb.toString();
  }
  
  private String getJobTypes() {
    return getEnumValues(JobCreator.values());
  }
  
  private String getSubmissionPolicies() {
    return getEnumValues(GridmixJobSubmissionPolicy.values());
  }
  
  protected void printUsage(PrintStream out) {
    ToolRunner.printGenericCommandUsage(out);
    out.println("Usage: gridmix [-generate <MiB>] [-users URI] [-Dname=value ...] <iopath> <trace>");
    out.println("  e.g. gridmix -generate 100m foo -");
    out.println("Options:");
    out.println("   -generate <MiB> : Generate input data of size MiB under "
        + "<iopath>/input/ and generate\n\t\t     distributed cache data under "
        + "<iopath>/distributedCache/.");
    out.println("   -users <usersResourceURI> : URI that contains the users list.");
    out.println("Configuration parameters:");
    out.println("   General parameters:");
    out.printf("       %-48s : Output directory\n", GRIDMIX_OUT_DIR);
    out.printf("       %-48s : Submitting threads\n", GRIDMIX_SUB_THR);
    out.printf("       %-48s : Queued job desc\n", GRIDMIX_QUE_DEP);
    out.printf("       %-48s : User resolution class\n", GRIDMIX_USR_RSV);
    out.printf("       %-48s : Job types (%s)\n", JobCreator.GRIDMIX_JOB_TYPE, getJobTypes());
    out.println("   Parameters related to job submission:");    
    out.printf("       %-48s : Default queue\n",
        GridmixJob.GRIDMIX_DEFAULT_QUEUE);
    out.printf("       %-48s : Enable/disable using queues in trace\n",
        GridmixJob.GRIDMIX_USE_QUEUE_IN_TRACE);
    out.printf("       %-48s : Job submission policy (%s)\n",
        GridmixJobSubmissionPolicy.JOB_SUBMISSION_POLICY, getSubmissionPolicies());
    out.println("   Parameters specific for LOADJOB:");
    out.printf("       %-48s : Key fraction of rec\n",
        AvgRecordFactory.GRIDMIX_KEY_FRC);
    out.println("   Parameters specific for SLEEPJOB:");
    out.printf("       %-48s : Whether to ignore reduce tasks\n",
        SleepJob.SLEEPJOB_MAPTASK_ONLY);
    out.printf("       %-48s : Number of fake locations for map tasks\n",
        JobCreator.SLEEPJOB_RANDOM_LOCATIONS);
    out.printf("       %-48s : Maximum map task runtime in mili-sec\n",
        SleepJob.GRIDMIX_SLEEP_MAX_MAP_TIME);
    out.printf("       %-48s : Maximum reduce task runtime in mili-sec (merge+reduce)\n",
        SleepJob.GRIDMIX_SLEEP_MAX_REDUCE_TIME);
    out.println("   Parameters specific for STRESS submission throttling policy:");
    out.printf("       %-48s : jobs vs task-tracker ratio\n",
        StressJobFactory.CONF_MAX_JOB_TRACKER_RATIO);
    out.printf("       %-48s : maps vs map-slot ratio\n",
        StressJobFactory.CONF_OVERLOAD_MAPTASK_MAPSLOT_RATIO);
    out.printf("       %-48s : reduces vs reduce-slot ratio\n",
        StressJobFactory.CONF_OVERLOAD_REDUCETASK_REDUCESLOT_RATIO);
    out.printf("       %-48s : map-slot share per job\n",
        StressJobFactory.CONF_MAX_MAPSLOT_SHARE_PER_JOB);
    out.printf("       %-48s : reduce-slot share per job\n",
        StressJobFactory.CONF_MAX_REDUCESLOT_SHARE_PER_JOB);
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
  // it is need for tests
  protected Summarizer getSummarizer() {
    return summarizer;
  }
  
}

