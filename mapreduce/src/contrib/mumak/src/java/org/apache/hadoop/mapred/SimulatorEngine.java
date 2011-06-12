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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.SimulatorEvent;
import org.apache.hadoop.mapred.SimulatorEventQueue;
import org.apache.hadoop.mapred.JobCompleteEvent;
import org.apache.hadoop.mapred.SimulatorJobClient;
import org.apache.hadoop.mapred.SimulatorJobTracker;
import org.apache.hadoop.mapred.SimulatorTaskTracker;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.tools.rumen.ClusterStory;
import org.apache.hadoop.tools.rumen.ClusterTopologyReader;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.LoggedNetworkTopology;
import org.apache.hadoop.tools.rumen.MachineNode;
import org.apache.hadoop.tools.rumen.RackNode;
import org.apache.hadoop.tools.rumen.ZombieCluster;
import org.apache.hadoop.tools.rumen.RandomSeedGenerator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * {@link SimulatorEngine} is the main class of the simulator. To launch the
 * simulator, user can either run the main class directly with two parameters,
 * input trace file and corresponding topology file, or use the script
 * "bin/mumak.sh trace.json topology.json". Trace file and topology file are
 * produced by rumen.
 */
public class SimulatorEngine extends Configured implements Tool {
  public static final List<SimulatorEvent> EMPTY_EVENTS = new ArrayList<SimulatorEvent>();
  /** Default number of milliseconds required to boot up the entire cluster. */
  public static final int DEFAULT_CLUSTER_STARTUP_DURATION = 100*1000;
  protected final SimulatorEventQueue queue = new SimulatorEventQueue();
  String traceFile;
  String topologyFile;
  SimulatorJobTracker jt;
  SimulatorJobClient jc;
  boolean shutdown = false;
  long terminateTime = Long.MAX_VALUE;
  long currentTime;
  /** 
   * Master random seed read from the configuration file, if present.
   * It is (only) used for creating sub seeds for all the random number 
   * generators.
   */
  long masterRandomSeed;
                                                                                                                                            
  /**
   * Start simulated task trackers based on topology.
   * @param clusterStory the cluster topology.
   * @param jobConf configuration object.
   * @param now
   *    time stamp when the simulator is started, {@link SimulatorTaskTracker}s
   *    are started uniformly randomly spread in [now,now+startDuration).
   * @return time stamp by which the entire cluster is booted up and all task
   *    trackers are sending hearbeats in their steady rate.
   */
  long startTaskTrackers(ClusterStory cluster, JobConf jobConf, long now) {
    /** port assigned to TTs, incremented by 1 for each TT */
    int port = 10000;
    int numTaskTrackers = 0;

    Random random = new Random(RandomSeedGenerator.getSeed(
       "forStartTaskTrackers()", masterRandomSeed));

    final int startDuration = jobConf.getInt("mumak.cluster.startup.duration",
        DEFAULT_CLUSTER_STARTUP_DURATION);
    
    for (MachineNode node : cluster.getMachines()) {
      jobConf.set("mumak.tasktracker.host.name", node.getName());
      jobConf.set("mumak.tasktracker.tracker.name",
          "tracker_" + node.getName() + ":localhost/127.0.0.1:" + port);
      long subRandomSeed = RandomSeedGenerator.getSeed(
         "forTaskTracker" + numTaskTrackers, masterRandomSeed);
      jobConf.setLong("mumak.tasktracker.random.seed", subRandomSeed);
      numTaskTrackers++;
      port++;
      SimulatorTaskTracker tt = new SimulatorTaskTracker(jt, jobConf);
      long firstHeartbeat = now + random.nextInt(startDuration);
      queue.addAll(tt.init(firstHeartbeat));
    }
    
    // In startDuration + heartbeat interval of the full cluster time each 
    // TT is started up and told on its 2nd heartbeat to beat at a rate 
    // corresponding to the steady state of the cluster    
    long clusterSteady = now + startDuration + jt.getNextHeartbeatInterval();
    return clusterSteady;
  }

  /**
   * Reads a positive long integer from a configuration.
   *
   * @param Configuration conf configuration objects
   * @param String propertyName name of the property
   * @return time
   */
  long getTimeProperty(Configuration conf, String propertyName,
                       long defaultValue) 
      throws IllegalArgumentException {
    // possible improvement: change date format to human readable ?
    long time = conf.getLong(propertyName, defaultValue);
    if (time <= 0) {
      throw new IllegalArgumentException(propertyName + "time must be positive: "
          + time);
    }
    return time;
  }
   
  /**
   * Initiate components in the simulation.
   * @throws InterruptedException
   * @throws IOException if trace or topology files cannot be open
   */
  @SuppressWarnings("deprecation")
  void init() throws InterruptedException, IOException {
    
    JobConf jobConf = new JobConf(getConf());
    jobConf.setClass("topology.node.switch.mapping.impl",
        StaticMapping.class, DNSToSwitchMapping.class);
    jobConf.set("fs.default.name", "file:///");
    jobConf.set("mapred.job.tracker", "localhost:8012");
    jobConf.setInt("mapred.jobtracker.job.history.block.size", 512);
    jobConf.setInt("mapred.jobtracker.job.history.buffer.size", 512);
    jobConf.setLong("mapred.tasktracker.expiry.interval", 5000);
    jobConf.setInt("mapred.reduce.copy.backoff", 4);
    jobConf.setLong("mapred.job.reuse.jvm.num.tasks", -1);
    jobConf.setUser("mumak");
    jobConf.set("mapred.system.dir", 
        jobConf.get("hadoop.log.dir", "/tmp/hadoop-"+jobConf.getUser()) + "/mapred/system");
    jobConf.set("mapred.jobtracker.taskScheduler", JobQueueTaskScheduler.class.getName());
    
    FileSystem lfs = FileSystem.getLocal(getConf());
    Path logPath =
      new Path(System.getProperty("hadoop.log.dir")).makeQualified(lfs);
    jobConf.set("mapred.system.dir", logPath.toString());
    jobConf.set("hadoop.job.history.location", (new Path(logPath, "history")
        .toString()));
    
    // start time for virtual clock
    // possible improvement: set default value to sth more meaningful based on
    // the 1st job
    long now = getTimeProperty(jobConf, "mumak.start.time", 
                               System.currentTimeMillis());

    jt = SimulatorJobTracker.startTracker(jobConf, now, this);
    jt.offerService();
    
    masterRandomSeed = jobConf.getLong("mumak.random.seed", System.nanoTime()); 
    
    // max Map/Reduce tasks per node
    int maxMaps = getConf().getInt(
        "mapred.tasktracker.map.tasks.maximum",
        SimulatorTaskTracker.DEFAULT_MAP_SLOTS);
    int maxReduces = getConf().getInt(
        "mapred.tasktracker.reduce.tasks.maximum",
    
      SimulatorTaskTracker.DEFAULT_REDUCE_SLOTS);

    MachineNode defaultNode = new MachineNode.Builder("default", 2)
        .setMapSlots(maxMaps).setReduceSlots(maxReduces).build();
            
    LoggedNetworkTopology topology = new ClusterTopologyReader(new Path(
        topologyFile), jobConf).get();
    // Setting the static mapping before removing numeric IP hosts.
    setStaticMapping(topology);
    if (getConf().getBoolean("mumak.topology.filter-numeric-ips", true)) {
      removeIpHosts(topology);
    }
    ZombieCluster cluster = new ZombieCluster(topology, defaultNode);
    
    // create TTs based on topology.json  
    long firstJobStartTime = startTaskTrackers(cluster, jobConf, now);

    long subRandomSeed = RandomSeedGenerator.getSeed("forSimulatorJobStoryProducer",
                                                     masterRandomSeed);
    JobStoryProducer jobStoryProducer = new SimulatorJobStoryProducer(
        new Path(traceFile), cluster, firstJobStartTime, jobConf, subRandomSeed);

    final SimulatorJobSubmissionPolicy submissionPolicy = SimulatorJobSubmissionPolicy
        .getPolicy(jobConf);

    jc = new SimulatorJobClient(jt, jobStoryProducer, submissionPolicy);
    queue.addAll(jc.init(firstJobStartTime));

    terminateTime = getTimeProperty(jobConf, "mumak.terminate.time",
                                    Long.MAX_VALUE);
  }
  
  /**
   * The main loop of the simulation. First call init() to get objects ready,
   * then go into the main loop, where {@link SimulatorEvent}s are handled removed from
   * the {@link SimulatorEventQueue}, and new {@link SimulatorEvent}s are created and inserted
   * into the {@link SimulatorEventQueue}.
   * @throws IOException
   * @throws InterruptedException
   */
  void run() throws IOException, InterruptedException {
    init();
    
    for (SimulatorEvent next = queue.get(); next != null
        && next.getTimeStamp() < terminateTime && !shutdown; next = queue.get()) {
      currentTime = next.getTimeStamp();
      assert(currentTime == queue.getCurrentTime());
      SimulatorEventListener listener = next.getListener();
      List<SimulatorEvent> response = listener.accept(next);
      queue.addAll(response);
    }
    
    summary(System.out);
  }
  
  /**
   * Run after the main loop.
   * @param out stream to output information about the simulation
   */
  void summary(PrintStream out) {
    out.println("Done, total events processed: " + queue.getEventCount());
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SimulatorEngine(), args);
    System.exit(res);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    parseParameters(args);
    try {
      run();
      return 0;
    } finally {
      if (jt != null) {
        jt.getTaskScheduler().terminate();
      }
    }
  }

  void parseParameters(String[] args) {
    if (args.length != 2) {
      throw new IllegalArgumentException("Usage: java ... SimulatorEngine trace.json topology.json");
    }
    traceFile = args[0];
    topologyFile = args[1];
  }

  /**
   * Called when a job is completed. Insert a {@link JobCompleteEvent} into the
   * {@link SimulatorEventQueue}. This event will be picked up by
   * {@link SimulatorJobClient}, which will in turn decide whether the
   * simulation is done.
   * @param jobStatus final status of a job, SUCCEEDED or FAILED
   * @param timestamp time stamp when the job is completed
   */
  void markCompletedJob(JobStatus jobStatus, long timestamp) {
    queue.add(new JobCompleteEvent(jc, timestamp, jobStatus, this));
  }

  /**
   * Called by {@link SimulatorJobClient} when the simulation is completed and
   * should be stopped.
   */
  void shutdown() {
    shutdown = true;
  }
  
  /**
   * Get the current virtual time of the on-going simulation. It is defined by
   * the time stamp of the last event handled.
   * @return the current virtual time
   */
  long getCurrentTime() {
    return currentTime;
  }
  
  // Due to HDFS-778, a node may appear in job history logs as both numeric
  // ips and as host names. We remove them from the parsed network topology
  // before feeding it to ZombieCluster.
  static void removeIpHosts(LoggedNetworkTopology topology) {
    for (Iterator<LoggedNetworkTopology> rackIt = topology.getChildren()
        .iterator(); rackIt.hasNext();) {
      LoggedNetworkTopology rack = rackIt.next();
      List<LoggedNetworkTopology> nodes = rack.getChildren();
      for (Iterator<LoggedNetworkTopology> it = nodes.iterator(); it.hasNext();) {
        LoggedNetworkTopology node = it.next();
        if (isIPAddress(node.getName())) {
          it.remove();
        }
      }
      if (nodes.isEmpty()) {
        rackIt.remove();
      }
    }
  }

  static Pattern IP_PATTERN;
  
  static {
    // 0-255
    String IPV4BK1 = "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
    // .b.c.d - where b/c/d are 0-255, and optionally adding two more
    // backslashes before each period
    String IPV4BKN = "(?:\\\\?\\." + IPV4BK1 + "){3}";
    String IPV4_PATTERN = IPV4BK1 + IPV4BKN;
    
    // first hexadecimal number
    String IPV6BK1 = "(?:[0-9a-fA-F]{1,4})";
    // remaining 7 hexadecimal numbers, each preceded with ":".
    String IPV6BKN = "(?::" + IPV6BK1 + "){7}";
    String IPV6_PATTERN = IPV6BK1 + IPV6BKN;

    IP_PATTERN = Pattern.compile(
        "^(?:" + IPV4_PATTERN + "|" + IPV6_PATTERN + ")$");
  }

 
  static boolean isIPAddress(String hostname) {
    return IP_PATTERN.matcher(hostname).matches();
  }
  
  static void setStaticMapping(LoggedNetworkTopology topology) {
    for (LoggedNetworkTopology rack : topology.getChildren()) {
      for (LoggedNetworkTopology node : rack.getChildren()) {
        StaticMapping.addNodeToRack(node.getName(), 
            new RackNode(rack.getName(), 1).getName());
      }
    }
  }
}
