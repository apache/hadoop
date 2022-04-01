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
import java.io.IOException;
import java.security.Security;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulator;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerWrapper;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.scheduler.Tracker;
import org.apache.hadoop.yarn.sls.synthetic.SynthTraceJobProducer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public class SLSRunner extends Configured implements Tool {
  private static final TaskRunner runner = new TaskRunner();
  private String[] inputTraces;

  // metrics
  private boolean printSimulation;

  private final static Map<String, Object> simulateInfoMap =
      new HashMap<>();

  // logger
  public final static Logger LOG = LoggerFactory.getLogger(SLSRunner.class);

  private static boolean exitAtTheFinish = false;
  private AMRunner amRunner;
  private RMRunner rmRunner;
  private NMRunner nmRunner;

  private TraceType inputType;
  private SynthTraceJobProducer stjp;

  /**
   * The type of trace in input.
   */
  public enum TraceType {
    SLS, RUMEN, SYNTH
  }

  public static final String NETWORK_CACHE_TTL = "networkaddress.cache.ttl";
  public static final String NETWORK_NEGATIVE_CACHE_TTL =
      "networkaddress.cache.negative.ttl";

  public int getRemainingApps() {
    return amRunner.remainingApps;
  }

  public SLSRunner() throws ClassNotFoundException, YarnException {
    Configuration tempConf = new Configuration(false);
    init(tempConf);
  }

  public SLSRunner(Configuration tempConf) throws ClassNotFoundException, YarnException {
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

  private void init(Configuration tempConf) throws ClassNotFoundException, YarnException {
    // runner configuration
    setConf(tempConf);
    
    int poolSize = tempConf.getInt(SLSConfiguration.RUNNER_POOL_SIZE,
        SLSConfiguration.RUNNER_POOL_SIZE_DEFAULT);
    SLSRunner.runner.setQueueSize(poolSize);

    rmRunner = new RMRunner(getConf(), this);
    nmRunner = new NMRunner(runner, getConf(), rmRunner.getRm(), rmRunner.getTableMapping(), poolSize);
    amRunner = new AMRunner(runner, this);
    amRunner.init(tempConf);
  }

  private SynthTraceJobProducer getSynthJobTraceProducer() throws YarnException {
    // if we use the nodeFile this could have been not initialized yet.
    if (nmRunner.getStjp() != null) {
      return nmRunner.getStjp();
    } else {
      try {
        return new SynthTraceJobProducer(getConf(), new Path(inputTraces[0]));
      } catch (IOException e) {
        throw new YarnException("Failed to initialize SynthTraceJobProducer", e);
      }
    }
  }

  /**
   * @return an unmodifiable view of the simulated info map.
   */
  public static Map<String, Object> getSimulateInfoMap() {
    return Collections.unmodifiableMap(simulateInfoMap);
  }

  /**
   * This is invoked before start.
   * @param inputType The trace type
   * @param inTraces Input traces
   * @param nodes The node file
   * @param metricsOutputDir Output dir for metrics
   * @param trackApps Track these applications
   * @param printSimulation Whether to print the simulation
   */
  public void setSimulationParams(TraceType inputType, String[] inTraces,
      String nodes, String metricsOutputDir, Set<String> trackApps,
      boolean printSimulation) throws YarnException {
    this.inputType = inputType;
    this.inputTraces = inTraces.clone();
    this.amRunner.setInputType(inputType);
    this.amRunner.setInputTraces(this.inputTraces);
    this.amRunner.setTrackedApps(trackApps);
    this.nmRunner.setNodeFile(nodes);
    this.nmRunner.setInputType(inputType);
    this.nmRunner.setInputTraces(this.inputTraces);
    this.printSimulation = printSimulation;
    this.rmRunner.setMetricsOutputDir(metricsOutputDir);
    String tableMapping = metricsOutputDir + "/tableMapping.csv";
    this.rmRunner.setTableMapping(tableMapping);
    this.nmRunner.setTableMapping(tableMapping);
    
    //We need this.inputTraces to set before creating SynthTraceJobProducer
    if (inputType == TraceType.SYNTH) {
      this.stjp = getSynthJobTraceProducer();
    }
  }

  public void start() throws IOException, ClassNotFoundException, YarnException,
      InterruptedException {
    enableDNSCaching(getConf());

    // start resource manager
    rmRunner.startRM();
    nmRunner.setRm(rmRunner.getRm());
    amRunner.setResourceManager(rmRunner.getRm());
    
    // start node managers
    nmRunner.startNM();
    // start application masters
    amRunner.startAM();

    // set queue & tracked apps information
    SchedulerWrapper resourceScheduler =
        (SchedulerWrapper) rmRunner.getRm().getResourceScheduler();
    resourceScheduler.setSLSRunner(this);
    Tracker tracker = resourceScheduler.getTracker();
    tracker.setQueueSet(rmRunner.getQueueAppNumMap().keySet());
    tracker.setTrackedAppSet(amRunner.getTrackedApps());
    // print out simulation info
    printSimulationInfo();
    // blocked until all nodes RUNNING
    nmRunner.waitForNodesRunning();
    // starting the runner once everything is ready to go,
    runner.start();
  }

  /**
   * Enables DNS Caching based on config. If DNS caching is enabled, then set
   * the DNS cache to infinite time. Since in SLS random nodes are added, DNS
   * resolution can take significant time which can cause erroneous results.
   * For more details, check <a href=
   * "https://docs.oracle.com/javase/8/docs/technotes/guides/net/properties.html">
   * Java Networking Properties</a>
   * @param conf Configuration object.
   */
  static void enableDNSCaching(Configuration conf) {
    if (conf.getBoolean(SLSConfiguration.DNS_CACHING_ENABLED,
        SLSConfiguration.DNS_CACHING_ENABLED_DEFAULT)) {
      Security.setProperty(NETWORK_CACHE_TTL, "-1");
      Security.setProperty(NETWORK_NEGATIVE_CACHE_TTL, "-1");
    }
  }

  Resource getDefaultContainerResource() {
    int containerMemory = getConf().getInt(SLSConfiguration.CONTAINER_MEMORY_MB,
        SLSConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
    int containerVCores = getConf().getInt(SLSConfiguration.CONTAINER_VCORES,
        SLSConfiguration.CONTAINER_VCORES_DEFAULT);
    return Resources.createResource(containerMemory, containerVCores);
  }

  public void increaseQueueAppNum(String queue) throws YarnException {
    rmRunner.increaseQueueAppNum(queue);
  }

  private void printSimulationInfo() {
    final int numAMs = amRunner.getNumAMs();
    final int numTasks = amRunner.getNumTasks();
    final long maxRuntime = amRunner.getMaxRuntime();
    Map<String, AMSimulator> amMap = amRunner.getAmMap();
    Map<String, Integer> queueAppNumMap = rmRunner.getQueueAppNumMap();

    if (printSimulation) {
      // node
      LOG.info("------------------------------------");
      LOG.info("# nodes = {}, # racks = {}, capacity " +
              "of each node {}.",
              nmRunner.getNumNMs(), nmRunner.getNumRacks(), nmRunner.getNodeManagerResource());
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
    simulateInfoMap.put("Number of racks", nmRunner.getNumRacks());
    simulateInfoMap.put("Number of nodes", nmRunner.getNumNMs());
    simulateInfoMap.put("Node memory (MB)",
        nmRunner.getNodeManagerResource().getResourceValue(ResourceInformation.MEMORY_URI));
    simulateInfoMap.put("Node VCores",
        nmRunner.getNodeManagerResource().getResourceValue(ResourceInformation.VCORES_URI));
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

  public Map<NodeId, NMSimulator> getNmMap() {
    return nmRunner.getNmMap();
  }

  public void decreaseRemainingApps() {
    amRunner.remainingApps--;
    if (amRunner.remainingApps == 0) {
      exitSLSRunner();
    }
  }

  public static void exitSLSRunner() {
    LOG.info("SLSRunner tears down.");
    if (exitAtTheFinish) {
      System.exit(0);
    }
  }

  public void stop() throws InterruptedException {
    rmRunner.stop();
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

    // compatibility with old commandline
    boolean hasInputRumenOption = cmd.hasOption("inputrumen");
    boolean hasInputSlsOption = cmd.hasOption("inputsls");
    boolean hasTraceTypeOption = cmd.hasOption("tracetype");
    TraceType traceType = determineTraceType(cmd, hasInputRumenOption,
        hasInputSlsOption, hasTraceTypeOption);
    String traceLocation = determineTraceLocation(cmd, hasInputRumenOption,
        hasInputSlsOption, hasTraceTypeOption);
    
    String output = cmd.getOptionValue("output");

    File outputFile = new File(output);
    if (!outputFile.exists() && !outputFile.mkdirs()) {
      System.err.println("ERROR: Cannot create output directory "
          + outputFile.getAbsolutePath());
      throw new YarnException("Cannot create output directory");
    }

    Set<String> trackedJobSet = new HashSet<>();
    if (cmd.hasOption("trackjobs")) {
      String trackjobs = cmd.getOptionValue("trackjobs");
      String[] jobIds = trackjobs.split(",");
      trackedJobSet.addAll(Arrays.asList(jobIds));
    }

    String tempNodeFile =
        cmd.hasOption("nodes") ? cmd.getOptionValue("nodes") : "";

    String[] inputFiles = traceLocation.split(",");

    setSimulationParams(traceType, inputFiles, tempNodeFile, output,
        trackedJobSet, cmd.hasOption("printsimulation"));
    
    start();

    return 0;
  }

  private TraceType determineTraceType(CommandLine cmd, boolean hasInputRumenOption,
      boolean hasInputSlsOption, boolean hasTraceTypeOption) throws YarnException {
    String traceType = null;
    if (hasInputRumenOption) {
      traceType = "RUMEN";
    }
    if (hasInputSlsOption) {
      traceType = "SLS";
    }
    if (hasTraceTypeOption) {
      traceType = cmd.getOptionValue("tracetype");
    }
    if (traceType == null) {
      throw new YarnException("Misconfigured input");
    }
    switch (traceType) {
    case "SLS":
      return TraceType.SLS;
    case "RUMEN":
      return TraceType.RUMEN;
    case "SYNTH":
      return TraceType.SYNTH;
    default:
      printUsage();
      throw new YarnException("Misconfigured input");
    }
  }

  private String determineTraceLocation(CommandLine cmd, boolean hasInputRumenOption,
      boolean hasInputSlsOption, boolean hasTraceTypeOption) throws YarnException {
    if (hasInputRumenOption) {
      return cmd.getOptionValue("inputrumen");
    }
    if (hasInputSlsOption) {
      return cmd.getOptionValue("inputsls");
    }
    if (hasTraceTypeOption) {
      return cmd.getOptionValue("tracelocation");
    }
    throw new YarnException("Misconfigured input! ");
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

  /**
   * Class to encapsulate all details about the node.
   */
  @Private
  @Unstable
  public static class NodeDetails {
    private String hostname;
    private Resource nodeResource;
    private Set<NodeLabel> labels;

    public NodeDetails(String nodeHostname) {
      this.hostname = nodeHostname;
    }

    public String getHostname() {
      return hostname;
    }

    public void setHostname(String hostname) {
      this.hostname = hostname;
    }

    public Resource getNodeResource() {
      return nodeResource;
    }

    public void setNodeResource(Resource nodeResource) {
      this.nodeResource = nodeResource;
    }

    public Set<NodeLabel> getLabels() {
      return labels;
    }

    public void setLabels(Set<NodeLabel> labels) {
      this.labels = labels;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof NodeDetails)) {
        return false;
      }

      NodeDetails that = (NodeDetails) o;

      return StringUtils.equals(hostname, that.hostname) && (
          Objects.equals(nodeResource, that.nodeResource)) && SetUtils
          .isEqualSet(labels, that.labels);
    }

    @Override
    public int hashCode() {
      int result = hostname == null ? 0 : hostname.hashCode();
      result =
          31 * result + (nodeResource == null ? 0 : nodeResource.hashCode());
      result = 31 * result + (labels == null ? 0 : labels.hashCode());
      return result;
    }
  }

  public SynthTraceJobProducer getStjp() {
    return stjp;
  }

  public void setStjp(SynthTraceJobProducer stjp) {
    this.stjp = stjp;
  }

  public AMSimulator getAMSimulatorByAppId(ApplicationId appId) {
    return amRunner.getAMSimulator(appId);
  }
}
