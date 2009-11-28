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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SimulatorEvent;
import org.apache.hadoop.mapred.SimulatorEventQueue;
import org.apache.hadoop.mapred.JobCompleteEvent;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.SimulatorJobClient;
import org.apache.hadoop.mapred.SimulatorJobTracker;
import org.apache.hadoop.mapred.SimulatorTaskTracker;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.tools.rumen.ClusterStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.MachineNode;
import org.apache.hadoop.tools.rumen.RackNode;
import org.apache.hadoop.tools.rumen.ZombieCluster;
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
  private static final int DEFAULT_MAP_SLOTS_PER_NODE = 2;
  private static final int DEFAULT_REDUCE_SLOTS_PER_NODE = 2;

  protected final SimulatorEventQueue queue = new SimulatorEventQueue();
  String traceFile;
  String topologyFile;
  SimulatorJobTracker jt;
  SimulatorJobClient jc;
  boolean shutdown = false;
  long terminateTime = Long.MAX_VALUE;
  long currentTime;
  
  /**
   * Start simulated task trackers based on topology.
   * @param clusterStory The cluster topology.
   * @param now
   *    time stamp when the simulator is started, {@link SimulatorTaskTracker}s
   *    are started shortly after this time stamp
   */
  void startTaskTrackers(ClusterStory clusterStory, long now) {
    /** port assigned to TTs, incremented by 1 for each TT */
    int port = 10000;
    long ms = now + 100;

    for (MachineNode node : clusterStory.getMachines()) {
      String hostname = node.getName();
      RackNode rackNode = node.getRackNode();
      StaticMapping.addNodeToRack(hostname, rackNode.getName());
      String taskTrackerName = "tracker_" + hostname + ":localhost/127.0.0.1:"
          + port;
      port++;
      SimulatorTaskTracker tt = new SimulatorTaskTracker(jt, taskTrackerName,
          hostname, node.getMapSlots(), node.getReduceSlots());
      queue.addAll(tt.init(ms++));
    }
  }
  
  /**
   * Initiate components in the simulation.
   * @throws InterruptedException
   * @throws IOException if trace or topology files cannot be open
   */
  @SuppressWarnings("deprecation")
  void init() throws InterruptedException, IOException {
    long now = System.currentTimeMillis();

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
    
    jt = SimulatorJobTracker.startTracker(jobConf, now, this);
    jt.offerService();
    
    // max Map/Reduce tasks per node
    int maxMaps = getConf().getInt("mapred.tasktracker.map.tasks.maximum",
        DEFAULT_MAP_SLOTS_PER_NODE);
    int maxReduces = getConf().getInt(
        "mapred.tasktracker.reduce.tasks.maximum",
        DEFAULT_REDUCE_SLOTS_PER_NODE);

    MachineNode defaultNode = new MachineNode.Builder("default", 2)
        .setMapSlots(maxMaps).setReduceSlots(maxReduces).build();
    ZombieCluster cluster = new ZombieCluster(new Path(topologyFile), 
        defaultNode, jobConf);
    long firstJobStartTime = now + 60000;
    JobStoryProducer jobStoryProducer = new SimulatorJobStoryProducer(
        new Path(traceFile), cluster, firstJobStartTime, jobConf);
    
    jc = new SimulatorJobClient(jt, jobStoryProducer);
    queue.addAll(jc.init(firstJobStartTime));

    // create TTs based on topology.json     
    startTaskTrackers(cluster, now);
    
    terminateTime = getConf().getLong("mumak.terminate.time", Long.MAX_VALUE);
    if (terminateTime <= 0) {
      throw new IllegalArgumentException("Terminate time must be positive: "
          + terminateTime);
    }
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
}
