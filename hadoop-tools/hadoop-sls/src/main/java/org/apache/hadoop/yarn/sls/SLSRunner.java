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
package org.apache.hadoop.yarn.sls;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulator;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.apache.hadoop.yarn.sls.resourcemanager.MockAMLauncher;
import org.apache.hadoop.yarn.sls.scheduler.SLSCapacityScheduler;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.scheduler.SLSFairScheduler;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerWrapper;
import org.apache.hadoop.yarn.sls.synthetic.SynthJob;
import org.apache.hadoop.yarn.sls.synthetic.SynthTraceJobProducer;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public class SLSRunner extends Configured implements Tool {
  // RM, Runner
  private ResourceManager rm;
  private static TaskRunner runner = new TaskRunner();
  private String[] inputTraces;
  private Map<String, Integer> queueAppNumMap;

  // NM simulator
  private HashMap<NodeId, NMSimulator> nmMap;
  private int nmMemoryMB, nmVCores;
  private String nodeFile;

  // AM simulator
  private int AM_ID;
  private Map<String, AMSimulator> amMap;
  private Set<String> trackedApps;
  private Map<String, Class> amClassMap;
  private static int remainingApps = 0;

  // metrics
  private String metricsOutputDir;
  private boolean printSimulation;

  // other simulation information
  private int numNMs, numRacks, numAMs, numTasks;
  private long maxRuntime;

  private final static Map<String, Object> simulateInfoMap =
          new HashMap<String, Object>();

  // logger
  public final static Logger LOG = LoggerFactory.getLogger(SLSRunner.class);

  private final static int DEFAULT_MAPPER_PRIORITY = 20;
  private final static int DEFAULT_REDUCER_PRIORITY = 10;

  private static boolean exitAtTheFinish = false;

  /**
   * The type of trace in input.
   */
  public enum TraceType {
    SLS, RUMEN, SYNTH
  }

  private TraceType inputType;
  private SynthTraceJobProducer stjp;

  public SLSRunner() throws ClassNotFoundException {
    Configuration tempConf = new Configuration(false);
    init(tempConf);
  }

  public SLSRunner(Configuration tempConf) throws ClassNotFoundException {
    init(tempConf);
  }

  @Override
  public void setConf(Configuration conf) {
    if (null != conf) {
      // Override setConf to make sure all conf added load sls-runner.xml, see
      // YARN-6560
      conf.addResource("sls-runner.xml");
    }
    super.setConf(conf);
  }

  private void init(Configuration tempConf) throws ClassNotFoundException {
    nmMap = new HashMap<>();
    queueAppNumMap = new HashMap<>();
    amMap = new ConcurrentHashMap<>();
    amClassMap = new HashMap<>();

    // runner configuration
    setConf(tempConf);

    // runner
    int poolSize = tempConf.getInt(SLSConfiguration.RUNNER_POOL_SIZE,
        SLSConfiguration.RUNNER_POOL_SIZE_DEFAULT);
    SLSRunner.runner.setQueueSize(poolSize);
    // <AMType, Class> map
    for (Map.Entry e : tempConf) {
      String key = e.getKey().toString();
      if (key.startsWith(SLSConfiguration.AM_TYPE)) {
        String amType = key.substring(SLSConfiguration.AM_TYPE.length());
        amClassMap.put(amType, Class.forName(tempConf.get(key)));
      }
    }
  }

  /**
   * @return an unmodifiable view of the simulated info map.
   */
  public static Map<String, Object> getSimulateInfoMap() {
    return Collections.unmodifiableMap(simulateInfoMap);
  }

  public void setSimulationParams(TraceType inType, String[] inTraces,
      String nodes, String outDir, Set<String> trackApps,
      boolean printsimulation) throws IOException, ClassNotFoundException {

    this.inputType = inType;
    this.inputTraces = inTraces.clone();
    this.nodeFile = nodes;
    this.trackedApps = trackApps;
    this.printSimulation = printsimulation;
    metricsOutputDir = outDir;

  }

  public void start() throws IOException, ClassNotFoundException, YarnException,
      InterruptedException {
    // start resource manager
    startRM();
    // start node managers
    startNM();
    // start application masters
    startAM();
    // set queue & tracked apps information
    ((SchedulerWrapper) rm.getResourceScheduler()).getTracker()
        .setQueueSet(this.queueAppNumMap.keySet());
    ((SchedulerWrapper) rm.getResourceScheduler()).getTracker()
        .setTrackedAppSet(this.trackedApps);
    // print out simulation info
    printSimulationInfo();
    // blocked until all nodes RUNNING
    waitForNodesRunning();
    // starting the runner once everything is ready to go,
    runner.start();
  }

  private void startRM() throws ClassNotFoundException, YarnException {
    Configuration rmConf = new YarnConfiguration(getConf());
    String schedulerClass = rmConf.get(YarnConfiguration.RM_SCHEDULER);

    if (Class.forName(schedulerClass) == CapacityScheduler.class) {
      rmConf.set(YarnConfiguration.RM_SCHEDULER,
          SLSCapacityScheduler.class.getName());
      rmConf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
      rmConf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
          ProportionalCapacityPreemptionPolicy.class.getName());
    } else if (Class.forName(schedulerClass) == FairScheduler.class) {
      rmConf.set(YarnConfiguration.RM_SCHEDULER,
          SLSFairScheduler.class.getName());
    } else if (Class.forName(schedulerClass) == FifoScheduler.class) {
      // TODO add support for FifoScheduler
      throw new YarnException("Fifo Scheduler is not supported yet.");
    }

    rmConf.set(SLSConfiguration.METRICS_OUTPUT_DIR, metricsOutputDir);

    final SLSRunner se = this;
    rm = new ResourceManager() {
      @Override
      protected ApplicationMasterLauncher createAMLauncher() {
        return new MockAMLauncher(se, this.rmContext, amMap);
      }
    };

    // Across runs of parametrized tests, the JvmMetrics objects is retained,
    // but is not registered correctly
    JvmMetrics jvmMetrics = JvmMetrics.initSingleton("ResourceManager", null);
    jvmMetrics.registerIfNeeded();

    // Init and start the actual ResourceManager
    rm.init(rmConf);
    rm.start();
  }

  private void startNM() throws YarnException, IOException {
    // nm configuration
    nmMemoryMB = getConf().getInt(SLSConfiguration.NM_MEMORY_MB,
        SLSConfiguration.NM_MEMORY_MB_DEFAULT);
    nmVCores = getConf().getInt(SLSConfiguration.NM_VCORES,
        SLSConfiguration.NM_VCORES_DEFAULT);
    int heartbeatInterval =
        getConf().getInt(SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS,
            SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    // nm information (fetch from topology file, or from sls/rumen json file)
    Set<String> nodeSet = new HashSet<String>();
    if (nodeFile.isEmpty()) {
      for (String inputTrace : inputTraces) {

        switch (inputType) {
        case SLS:
          nodeSet.addAll(SLSUtils.parseNodesFromSLSTrace(inputTrace));
          break;
        case RUMEN:
          nodeSet.addAll(SLSUtils.parseNodesFromRumenTrace(inputTrace));
          break;
        case SYNTH:
          stjp = new SynthTraceJobProducer(getConf(), new Path(inputTraces[0]));
          nodeSet.addAll(SLSUtils.generateNodes(stjp.getNumNodes(),
              stjp.getNumNodes()/stjp.getNodesPerRack()));
          break;
        default:
          throw new YarnException("Input configuration not recognized, "
              + "trace type should be SLS, RUMEN, or SYNTH");
        }
      }
    } else {
      nodeSet.addAll(SLSUtils.parseNodesFromNodeFile(nodeFile));
    }

    if (nodeSet.size() == 0) {
      throw new YarnException("No node! Please configure nodes.");
    }

    // create NM simulators
    Random random = new Random();
    Set<String> rackSet = new HashSet<String>();
    for (String hostName : nodeSet) {
      // we randomize the heartbeat start time from zero to 1 interval
      NMSimulator nm = new NMSimulator();
      nm.init(hostName, nmMemoryMB, nmVCores, random.nextInt(heartbeatInterval),
          heartbeatInterval, rm);
      nmMap.put(nm.getNode().getNodeID(), nm);
      runner.schedule(nm);
      rackSet.add(nm.getNode().getRackName());
    }
    numRacks = rackSet.size();
    numNMs = nmMap.size();
  }

  private void waitForNodesRunning() throws InterruptedException {
    long startTimeMS = System.currentTimeMillis();
    while (true) {
      int numRunningNodes = 0;
      for (RMNode node : rm.getRMContext().getRMNodes().values()) {
        if (node.getState() == NodeState.RUNNING) {
          numRunningNodes++;
        }
      }
      if (numRunningNodes == numNMs) {
        break;
      }
      LOG.info("SLSRunner is waiting for all nodes RUNNING."
          + " {} of {} NMs initialized.", numRunningNodes, numNMs);
      Thread.sleep(1000);
    }
    LOG.info("SLSRunner takes {} ms to launch all nodes.",
        System.currentTimeMillis() - startTimeMS);
  }

  @SuppressWarnings("unchecked")
  private void startAM() throws YarnException, IOException {
    switch (inputType) {
    case SLS:
      for (String inputTrace : inputTraces) {
        startAMFromSLSTrace(inputTrace);
      }
      break;
    case RUMEN:
      long baselineTimeMS = 0;
      for (String inputTrace : inputTraces) {
        startAMFromRumenTrace(inputTrace, baselineTimeMS);
      }
      break;
    case SYNTH:
      startAMFromSynthGenerator();
      break;
    default:
      throw new YarnException("Input configuration not recognized, "
          + "trace type should be SLS, RUMEN, or SYNTH");
    }

    numAMs = amMap.size();
    remainingApps = numAMs;
  }

  /**
   * Parse workload from a SLS trace file.
   */
  @SuppressWarnings("unchecked")
  private void startAMFromSLSTrace(String inputTrace) throws IOException {
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();

    try (Reader input = new InputStreamReader(
        new FileInputStream(inputTrace), "UTF-8")) {
      Iterator<Map> jobIter = mapper.readValues(
          jsonF.createParser(input), Map.class);

      while (jobIter.hasNext()) {
        try {
          createAMForJob(jobIter.next());
        } catch (Exception e) {
          LOG.error("Failed to create an AM: {}", e.getMessage());
        }
      }
    }
  }

  private void createAMForJob(Map jsonJob) throws YarnException {
    long jobStartTime = Long.parseLong(jsonJob.get("job.start.ms").toString());

    long jobFinishTime = 0;
    if (jsonJob.containsKey("job.end.ms")) {
      jobFinishTime = Long.parseLong(jsonJob.get("job.end.ms").toString());
    }

    String user = (String) jsonJob.get("job.user");
    if (user == null) {
      user = "default";
    }

    String queue = jsonJob.get("job.queue.name").toString();
    increaseQueueAppNum(queue);

    String amType = (String)jsonJob.get("am.type");
    if (amType == null) {
      amType = SLSUtils.DEFAULT_JOB_TYPE;
    }

    int jobCount = 1;
    if (jsonJob.containsKey("job.count")) {
      jobCount = Integer.parseInt(jsonJob.get("job.count").toString());
    }
    jobCount = Math.max(jobCount, 1);

    String oldAppId = (String)jsonJob.get("job.id");
    // Job id is generated automatically if this job configuration allows
    // multiple job instances
    if(jobCount > 1) {
      oldAppId = null;
    }

    for (int i = 0; i < jobCount; i++) {
      runNewAM(amType, user, queue, oldAppId, jobStartTime, jobFinishTime,
          getTaskContainers(jsonJob), null, getAMContainerResource(jsonJob));
    }
  }

  private List<ContainerSimulator> getTaskContainers(Map jsonJob)
      throws YarnException {
    List<ContainerSimulator> containers = new ArrayList<>();
    List tasks = (List) jsonJob.get("job.tasks");
    if (tasks == null || tasks.size() == 0) {
      throw new YarnException("No task for the job!");
    }

    for (Object o : tasks) {
      Map jsonTask = (Map) o;

      String hostname = (String) jsonTask.get("container.host");

      long duration = 0;
      if (jsonTask.containsKey("duration.ms")) {
        duration = Integer.parseInt(jsonTask.get("duration.ms").toString());
      } else if (jsonTask.containsKey("container.start.ms") &&
          jsonTask.containsKey("container.end.ms")) {
        long taskStart = Long.parseLong(jsonTask.get("container.start.ms")
            .toString());
        long taskFinish = Long.parseLong(jsonTask.get("container.end.ms")
            .toString());
        duration = taskFinish - taskStart;
      }
      if (duration <= 0) {
        throw new YarnException("Duration of a task shouldn't be less or equal"
            + " to 0!");
      }

      Resource res = getDefaultContainerResource();
      if (jsonTask.containsKey("container.memory")) {
        int containerMemory =
            Integer.parseInt(jsonTask.get("container.memory").toString());
        res.setMemorySize(containerMemory);
      }

      if (jsonTask.containsKey("container.vcores")) {
        int containerVCores =
            Integer.parseInt(jsonTask.get("container.vcores").toString());
        res.setVirtualCores(containerVCores);
      }

      int priority = DEFAULT_MAPPER_PRIORITY;
      if (jsonTask.containsKey("container.priority")) {
        priority = Integer.parseInt(jsonTask.get("container.priority")
            .toString());
      }

      String type = "map";
      if (jsonTask.containsKey("container.type")) {
        type = jsonTask.get("container.type").toString();
      }

      int count = 1;
      if (jsonTask.containsKey("count")) {
        count = Integer.parseInt(jsonTask.get("count").toString());
      }
      count = Math.max(count, 1);

      for (int i = 0; i < count; i++) {
        containers.add(
            new ContainerSimulator(res, duration, hostname, priority, type));
      }
    }

    return containers;
  }

  /**
   * Parse workload from a rumen trace file.
   */
  @SuppressWarnings("unchecked")
  private void startAMFromRumenTrace(String inputTrace, long baselineTimeMS)
      throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    File fin = new File(inputTrace);

    try (JobTraceReader reader = new JobTraceReader(
        new Path(fin.getAbsolutePath()), conf)) {
      LoggedJob job = reader.getNext();

      while (job != null) {
        try {
          createAMForJob(job, baselineTimeMS);
        } catch (Exception e) {
          LOG.error("Failed to create an AM: {}", e.getMessage());
        }

        job = reader.getNext();
      }
    }
  }

  private void createAMForJob(LoggedJob job, long baselineTimeMs)
      throws YarnException {
    String user = job.getUser() == null ? "default" :
        job.getUser().getValue();
    String jobQueue = job.getQueue().getValue();
    String oldJobId = job.getJobID().toString();
    long jobStartTimeMS = job.getSubmitTime();
    long jobFinishTimeMS = job.getFinishTime();
    if (baselineTimeMs == 0) {
      baselineTimeMs = job.getSubmitTime();
    }
    jobStartTimeMS -= baselineTimeMs;
    jobFinishTimeMS -= baselineTimeMs;
    if (jobStartTimeMS < 0) {
      LOG.warn("Warning: reset job {} start time to 0.", oldJobId);
      jobFinishTimeMS = jobFinishTimeMS - jobStartTimeMS;
      jobStartTimeMS = 0;
    }

    increaseQueueAppNum(jobQueue);

    List<ContainerSimulator> containerList = new ArrayList<>();
    // mapper
    for (LoggedTask mapTask : job.getMapTasks()) {
      if (mapTask.getAttempts().size() == 0) {
        throw new YarnException("Invalid map task, no attempt for a mapper!");
      }
      LoggedTaskAttempt taskAttempt =
          mapTask.getAttempts().get(mapTask.getAttempts().size() - 1);
      String hostname = taskAttempt.getHostName().getValue();
      long containerLifeTime = taskAttempt.getFinishTime() -
          taskAttempt.getStartTime();
      containerList.add(
          new ContainerSimulator(getDefaultContainerResource(),
              containerLifeTime, hostname, DEFAULT_MAPPER_PRIORITY, "map"));
    }

    // reducer
    for (LoggedTask reduceTask : job.getReduceTasks()) {
      if (reduceTask.getAttempts().size() == 0) {
        throw new YarnException(
            "Invalid reduce task, no attempt for a reducer!");
      }
      LoggedTaskAttempt taskAttempt =
          reduceTask.getAttempts().get(reduceTask.getAttempts().size() - 1);
      String hostname = taskAttempt.getHostName().getValue();
      long containerLifeTime = taskAttempt.getFinishTime() -
          taskAttempt.getStartTime();
      containerList.add(
          new ContainerSimulator(getDefaultContainerResource(),
              containerLifeTime, hostname, DEFAULT_REDUCER_PRIORITY, "reduce"));
    }

    // Only supports the default job type currently
    runNewAM(SLSUtils.DEFAULT_JOB_TYPE, user, jobQueue, oldJobId,
        jobStartTimeMS, jobFinishTimeMS, containerList, null,
        getAMContainerResource(null));
  }

  private Resource getDefaultContainerResource() {
    int containerMemory = getConf().getInt(SLSConfiguration.CONTAINER_MEMORY_MB,
        SLSConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
    int containerVCores = getConf().getInt(SLSConfiguration.CONTAINER_VCORES,
        SLSConfiguration.CONTAINER_VCORES_DEFAULT);
    return Resources.createResource(containerMemory, containerVCores);
  }

  /**
   * parse workload information from synth-generator trace files.
   */
  @SuppressWarnings("unchecked")
  private void startAMFromSynthGenerator() throws YarnException, IOException {
    Configuration localConf = new Configuration();
    localConf.set("fs.defaultFS", "file:///");
    long baselineTimeMS = 0;

    // reservations use wall clock time, so need to have a reference for that
    UTCClock clock = new UTCClock();
    long now = clock.getTime();

    try {

      // if we use the nodeFile this could have been not initialized yet.
      if (stjp == null) {
        stjp = new SynthTraceJobProducer(getConf(), new Path(inputTraces[0]));
      }

      SynthJob job = null;
      // we use stjp, a reference to the job producer instantiated during node
      // creation
      while ((job = (SynthJob) stjp.getNextJob()) != null) {
        // only support MapReduce currently
        String user = job.getUser();
        String jobQueue = job.getQueueName();
        String oldJobId = job.getJobID().toString();
        long jobStartTimeMS = job.getSubmissionTime();

        // CARLO: Finish time is only used for logging, omit for now
        long jobFinishTimeMS = -1L;

        if (baselineTimeMS == 0) {
          baselineTimeMS = jobStartTimeMS;
        }
        jobStartTimeMS -= baselineTimeMS;
        jobFinishTimeMS -= baselineTimeMS;
        if (jobStartTimeMS < 0) {
          LOG.warn("Warning: reset job {} start time to 0.", oldJobId);
          jobFinishTimeMS = jobFinishTimeMS - jobStartTimeMS;
          jobStartTimeMS = 0;
        }

        increaseQueueAppNum(jobQueue);

        List<ContainerSimulator> containerList =
            new ArrayList<ContainerSimulator>();
        ArrayList<NodeId> keyAsArray = new ArrayList<NodeId>(nmMap.keySet());
        Random rand = new Random(stjp.getSeed());

        Resource maxMapRes = Resource.newInstance(0, 0);
        long maxMapDur = 0;
        // map tasks
        for (int i = 0; i < job.getNumberMaps(); i++) {
          TaskAttemptInfo tai = job.getTaskAttemptInfo(TaskType.MAP, i, 0);
          RMNode node = nmMap
              .get(keyAsArray.get(rand.nextInt(keyAsArray.size()))).getNode();
          String hostname = "/" + node.getRackName() + "/" + node.getHostName();
          long containerLifeTime = tai.getRuntime();
          Resource containerResource =
              Resource.newInstance((int) tai.getTaskInfo().getTaskMemory(),
                  (int) tai.getTaskInfo().getTaskVCores());
          containerList.add(new ContainerSimulator(containerResource,
              containerLifeTime, hostname, DEFAULT_MAPPER_PRIORITY, "map"));
          maxMapRes = Resources.componentwiseMax(maxMapRes, containerResource);
          maxMapDur =
              containerLifeTime > maxMapDur ? containerLifeTime : maxMapDur;

        }

        Resource maxRedRes = Resource.newInstance(0, 0);
        long maxRedDur = 0;
        // reduce tasks
        for (int i = 0; i < job.getNumberReduces(); i++) {
          TaskAttemptInfo tai = job.getTaskAttemptInfo(TaskType.REDUCE, i, 0);
          RMNode node = nmMap
              .get(keyAsArray.get(rand.nextInt(keyAsArray.size()))).getNode();
          String hostname = "/" + node.getRackName() + "/" + node.getHostName();
          long containerLifeTime = tai.getRuntime();
          Resource containerResource =
              Resource.newInstance((int) tai.getTaskInfo().getTaskMemory(),
                  (int) tai.getTaskInfo().getTaskVCores());
          containerList.add(new ContainerSimulator(containerResource,
              containerLifeTime, hostname, DEFAULT_REDUCER_PRIORITY, "reduce"));
          maxRedRes = Resources.componentwiseMax(maxRedRes, containerResource);
          maxRedDur =
              containerLifeTime > maxRedDur ? containerLifeTime : maxRedDur;

        }

        // generating reservations for the jobs that require them

        ReservationSubmissionRequest rr = null;
        if (job.hasDeadline()) {
          ReservationId reservationId =
              ReservationId.newInstance(this.rm.getStartTime(), AM_ID);

          rr = ReservationClientUtil.createMRReservation(reservationId,
              "reservation_" + AM_ID, maxMapRes, job.getNumberMaps(), maxMapDur,
              maxRedRes, job.getNumberReduces(), maxRedDur,
              now + jobStartTimeMS, now + job.getDeadline(),
              job.getQueueName());

        }

        runNewAM(SLSUtils.DEFAULT_JOB_TYPE, user, jobQueue, oldJobId,
            jobStartTimeMS, jobFinishTimeMS, containerList, rr,
            getAMContainerResource(null));
      }
    } finally {
      stjp.close();
    }

  }

  private Resource getAMContainerResource(Map jsonJob) {
    Resource amContainerResource =
        SLSConfiguration.getAMContainerResource(getConf());

    if (jsonJob == null) {
      return amContainerResource;
    }

    if (jsonJob.containsKey("am.memory")) {
      amContainerResource.setMemorySize(
          Long.parseLong(jsonJob.get("am.memory").toString()));
    }

    if (jsonJob.containsKey("am.vcores")) {
      amContainerResource.setVirtualCores(
          Integer.parseInt(jsonJob.get("am.vcores").toString()));
    }
    return amContainerResource;
  }

  private void increaseQueueAppNum(String queue) throws YarnException {
    SchedulerWrapper wrapper = (SchedulerWrapper)rm.getResourceScheduler();
    String queueName = wrapper.getRealQueueName(queue);
    Integer appNum = queueAppNumMap.get(queueName);
    if (appNum == null) {
      appNum = 1;
    } else {
      appNum++;
    }

    queueAppNumMap.put(queueName, appNum);
  }

  private void runNewAM(String jobType, String user,
      String jobQueue, String oldJobId, long jobStartTimeMS,
      long jobFinishTimeMS, List<ContainerSimulator> containerList,
      ReservationSubmissionRequest rr, Resource amContainerResource) {

    AMSimulator amSim = (AMSimulator) ReflectionUtils.newInstance(
        amClassMap.get(jobType), new Configuration());

    if (amSim != null) {
      int heartbeatInterval = getConf().getInt(
          SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS,
          SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
      boolean isTracked = trackedApps.contains(oldJobId);

      if (oldJobId == null) {
        oldJobId = Integer.toString(AM_ID);
      }
      AM_ID++;

      amSim.init(heartbeatInterval, containerList, rm, this, jobStartTimeMS,
          jobFinishTimeMS, user, jobQueue, isTracked, oldJobId, rr,
          runner.getStartTimeMS(), amContainerResource);
      runner.schedule(amSim);
      maxRuntime = Math.max(maxRuntime, jobFinishTimeMS);
      numTasks += containerList.size();
      amMap.put(oldJobId, amSim);
    }
  }

  private void printSimulationInfo() {
    if (printSimulation) {
      // node
      LOG.info("------------------------------------");
      LOG.info("# nodes = {}, # racks = {}, capacity " +
              "of each node {} MB memory and {} vcores.",
              numNMs, numRacks, nmMemoryMB, nmVCores);
      LOG.info("------------------------------------");
      // job
      LOG.info("# applications = {}, # total " +
              "tasks = {}, average # tasks per application = {}",
              numAMs, numTasks, (int)(Math.ceil((numTasks + 0.0) / numAMs)));
      LOG.info("JobId\tQueue\tAMType\tDuration\t#Tasks");
      for (Map.Entry<String, AMSimulator> entry : amMap.entrySet()) {
        AMSimulator am = entry.getValue();
        LOG.info(entry.getKey() + "\t" + am.getQueue() + "\t" + am.getAMType()
            + "\t" + am.getDuration() + "\t" + am.getNumTasks());
      }
      LOG.info("------------------------------------");
      // queue
      LOG.info("number of queues = {}  average number of apps = {}",
          queueAppNumMap.size(),
          (int)(Math.ceil((numAMs + 0.0) / queueAppNumMap.size())));
      LOG.info("------------------------------------");
      // runtime
      LOG.info("estimated simulation time is {} seconds",
          (long)(Math.ceil(maxRuntime / 1000.0)));
      LOG.info("------------------------------------");
    }
    // package these information in the simulateInfoMap used by other places
    simulateInfoMap.put("Number of racks", numRacks);
    simulateInfoMap.put("Number of nodes", numNMs);
    simulateInfoMap.put("Node memory (MB)", nmMemoryMB);
    simulateInfoMap.put("Node VCores", nmVCores);
    simulateInfoMap.put("Number of applications", numAMs);
    simulateInfoMap.put("Number of tasks", numTasks);
    simulateInfoMap.put("Average tasks per applicaion",
            (int)(Math.ceil((numTasks + 0.0) / numAMs)));
    simulateInfoMap.put("Number of queues", queueAppNumMap.size());
    simulateInfoMap.put("Average applications per queue",
            (int)(Math.ceil((numAMs + 0.0) / queueAppNumMap.size())));
    simulateInfoMap.put("Estimated simulate time (s)",
            (long)(Math.ceil(maxRuntime / 1000.0)));
  }

  public HashMap<NodeId, NMSimulator> getNmMap() {
    return nmMap;
  }

  public static void decreaseRemainingApps() {
    remainingApps--;

    if (remainingApps == 0) {
      LOG.info("SLSRunner tears down.");
      if (exitAtTheFinish) {
        System.exit(0);
      }
    }
  }

  public void stop() throws InterruptedException {
    rm.stop();
    runner.stop();
  }

  public int run(final String[] argv) throws IOException, InterruptedException,
      ParseException, ClassNotFoundException, YarnException {

    Options options = new Options();

    // Left for compatibility
    options.addOption("inputrumen", true, "input rumen files");
    options.addOption("inputsls", true, "input sls files");

    // New more general format
    options.addOption("tracetype", true, "the type of trace");
    options.addOption("tracelocation", true, "input trace files");

    options.addOption("nodes", true, "input topology");
    options.addOption("output", true, "output directory");
    options.addOption("trackjobs", true,
        "jobs to be tracked during simulating");
    options.addOption("printsimulation", false,
        "print out simulation information");

    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, argv);

    String traceType = null;
    String traceLocation = null;

    // compatibility with old commandline
    if (cmd.hasOption("inputrumen")) {
      traceType = "RUMEN";
      traceLocation = cmd.getOptionValue("inputrumen");
    }
    if (cmd.hasOption("inputsls")) {
      traceType = "SLS";
      traceLocation = cmd.getOptionValue("inputsls");
    }

    if (cmd.hasOption("tracetype")) {
      traceType = cmd.getOptionValue("tracetype");
      traceLocation = cmd.getOptionValue("tracelocation");
    }

    String output = cmd.getOptionValue("output");

    File outputFile = new File(output);
    if (!outputFile.exists() && !outputFile.mkdirs()) {
      System.err.println("ERROR: Cannot create output directory "
          + outputFile.getAbsolutePath());
      throw new YarnException("Cannot create output directory");
    }

    Set<String> trackedJobSet = new HashSet<String>();
    if (cmd.hasOption("trackjobs")) {
      String trackjobs = cmd.getOptionValue("trackjobs");
      String jobIds[] = trackjobs.split(",");
      trackedJobSet.addAll(Arrays.asList(jobIds));
    }

    String tempNodeFile =
        cmd.hasOption("nodes") ? cmd.getOptionValue("nodes") : "";

    TraceType tempTraceType = TraceType.SLS;
    switch (traceType) {
    case "SLS":
      tempTraceType = TraceType.SLS;
      break;
    case "RUMEN":
      tempTraceType = TraceType.RUMEN;
      break;

    case "SYNTH":
      tempTraceType = TraceType.SYNTH;
      break;
    default:
      printUsage();
      throw new YarnException("Misconfigured input");
    }

    String[] inputFiles = traceLocation.split(",");

    setSimulationParams(tempTraceType, inputFiles, tempNodeFile, output,
        trackedJobSet, cmd.hasOption("printsimulation"));

    start();

    return 0;
  }

  public static void main(String[] argv) throws Exception {
    exitAtTheFinish = true;
    ToolRunner.run(new Configuration(), new SLSRunner(), argv);
  }

  static void printUsage() {
    System.err.println();
    System.err.println("ERROR: Wrong tracetype");
    System.err.println();
    System.err.println(
        "Options: -tracetype " + "SLS|RUMEN|SYNTH -tracelocation FILE,FILE... "
            + "(deprecated alternative options --inputsls FILE, FILE,... "
            + " | --inputrumen FILE,FILE,...)"
            + "-output FILE [-nodes FILE] [-trackjobs JobId,JobId...] "
            + "[-printsimulation]");
    System.err.println();
  }

}
