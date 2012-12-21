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

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSClusterWithNodeGroup;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.net.StaticMapping;
import org.junit.BeforeClass;

public class TestNodeGroupAwareTaskPlacement extends TestCase {

  private static final String rack1[] = new String[] {
    "/r1"
  };
  private static final String nodeGroup1[] = new String[] {
    "/nodegroup1"
  };
  private static final String hosts1[] = new String[] {
    "host1.nodegroup1.rack1"
  };
  private static final String rack2[] = new String[] {
    "/r1", "/r2"
  };
  private static final String nodeGroup2[] = new String[] {
    "/nodegroup2", "/nodegroup3"
  };
  private static final String hosts2[] = new String[] {
    "host2.nodegroup2.rack1", "host2.nodegroup3.rack2"
  };
  private static final String hosts3[] = new String[] {
    "host2.nodegroup3.rack2"
  };
  private static final String nodeGroup3[] = new String[] {
    "/nodegroup3"
  };
  private static final String rack3[] = new String[] {
    "/r2"
  };
  private static final String hosts4[] = new String[] {
    "host3.nodegroup1.rack1"
  };
  private static final String nodeGroup4[] = new String[] {
    "/nodegroup1"
  };
  private static final String rack4[] = new String[] {
    "/r1"
  };
  final Path inDir = new Path("/nodegrouptesting");
  final Path outputPath = new Path("/output");

  /**
   * Launches a MR job and tests the job counters against the expected values.
   * @param testName The name for the job
   * @param mr The MR cluster
   * @param fileSys The FileSystem
   * @param in Input path
   * @param out Output path
   * @param numMaps Number of maps
   * @param otherLocalMaps Expected value of other local maps
   * @param rackLocalMaps Expected value of rack local maps
   * @param nodeGroupLocalMaps Expected value of nodeGroup local maps
   * @param dataLocalMaps Expected value of data(node) local maps
   * @param jobConfig Configuration for running job
   */
  static void launchJobAndTestCounters(String jobName, MiniMRCluster mr,
                                       FileSystem fileSys, Path in, Path out,
                                       int numMaps, int otherLocalMaps,
                                       int rackLocalMaps, int nodeGroupLocalMaps,
                                       int dataLocalMaps, JobConf jobConfig)
  throws IOException {
    JobConf jobConf = mr.createJobConf(jobConfig);
    if (fileSys.exists(out)) {
        fileSys.delete(out, true);
    }
    RunningJob job = launchJob(jobConf, in, out, numMaps, jobName);
    Counters counters = job.getCounters();
    assertEquals("Number of local maps",
            counters.getCounter(JobInProgress.Counter.OTHER_LOCAL_MAPS), otherLocalMaps);
    assertEquals("Number of Data-local maps",
            counters.getCounter(JobInProgress.Counter.DATA_LOCAL_MAPS),
                                dataLocalMaps);
    assertEquals("Number of NodeGroup-local maps",
            counters.getCounter(JobInProgress.Counter.NODEGROUP_LOCAL_MAPS),
                                nodeGroupLocalMaps);
    assertEquals("Number of Rack-local maps",
            counters.getCounter(JobInProgress.Counter.RACK_LOCAL_MAPS),
                                rackLocalMaps);

    mr.waitUntilIdle();
    mr.shutdown();
  }

  @BeforeClass
  public void setUp(){
    // map host to related locations
    StaticMapping.addNodeToRack(hosts1[0], rack1[0]+nodeGroup1[0]);
    StaticMapping.addNodeToRack(hosts2[0], rack2[0]+nodeGroup2[0]);
    StaticMapping.addNodeToRack(hosts2[1], rack2[1]+nodeGroup2[1]);
    StaticMapping.addNodeToRack(hosts4[0], rack4[0]+nodeGroup4[0]);
  }

  public void testTaskPlacement() throws IOException {
    String namenode = null;
    MiniDFSClusterWithNodeGroup dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    String testName = "TestForNodeGroupAwareness";
    try {
      final int taskTrackers = 1;

      /* Start 4 datanodes, two in rack r1/nodegroup1, one in r1/nodegroup2 and
       * the other one in r2/nodegroup3. Create three
       * files (splits).
       * 1) file1, just after starting the datanode on r1/nodegroup1, with
       *    a repl factor of 1, and,
       * 2) file2 & file3 after starting the two datanodes in r1/nodegroup2 and
       *    r2/nodegroup3, with a repl factor of 3.
       * 3) start the last data node (datanode4) in r1/nodegroup1
       * At the end, file1 will be present on only datanode1, and, file2 and
       * file3, will be present on all datanodes except datanode4.
       */
      Configuration conf = new Configuration();
      conf.setBoolean("dfs.replication.considerLoad", false);

      conf.set("dfs.block.replicator.classname",
          "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyWithNodeGroup");

      conf.set("net.topology.impl",
          "org.apache.hadoop.net.NetworkTopologyWithNodeGroup");

      conf.setBoolean("net.topology.nodegroup.aware", true);

      conf.setBoolean("mapred.jobtracker.nodegroup.aware", true);
      conf.setInt("mapred.task.cache.levels", 3);

      conf.set("mapred.jobtracker.jobSchedulable",
          "org.apache.hadoop.mapred.JobSchedulableWithNodeGroup");

      JobConf jobConf = new JobConf(conf);

      MiniDFSClusterWithNodeGroup.setNodeGroups(nodeGroup1);
      // start the dfs cluster with datanode1 only.
      dfs = new MiniDFSClusterWithNodeGroup(0, conf, 1,
                true, true, null, rack1, hosts1, null);

      dfs.waitActive();
      fileSys = dfs.getFileSystem();
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }
      // write file1 on datanode1 with 1 replica
      UtilsForTests.writeFile(
          dfs.getNameNode(), conf, new Path(inDir + "/file1"), (short)1);
      // start another two datanodes (2 and 3)
      dfs.startDataNodes(conf, 2, true, null, rack2, nodeGroup2, hosts2, null);

      dfs.waitActive();
      // write two files with 3 replica, so each datanodes will have one replica
      // of file2 and file3
      UtilsForTests.writeFile(
          dfs.getNameNode(), conf, new Path(inDir + "/file2"), (short)3);
      UtilsForTests.writeFile(
          dfs.getNameNode(), conf, new Path(inDir + "/file3"), (short)3);

      namenode = (dfs.getFileSystem()).getUri().getHost() + ":" +
                 (dfs.getFileSystem()).getUri().getPort();
      /* Run a job with the (only)tasktracker which is under r2/nodegroup3 and
       * check the task placement that how many data/nodegroup/rack local maps
       *  it runs. The hostname of the tasktracker is set to same as datanode3.
       */
      mr = new MiniMRClusterWithNodeGroup(taskTrackers, namenode, 1, rack3,
          nodeGroup3, hosts3, jobConf);
      /* The job is configured with three maps since there are three
       * (non-splittable) files. On rack2, there are two files and both
       * have repl of three. The blocks for those files must therefore be
       * present on all the datanodes (except datanode4), in particular,
       * the datanode3 on rack2. The third input file is pulled from rack1,
       * thus the result should be 2 rack-local maps.
       */
      launchJobAndTestCounters(testName, mr, fileSys, inDir, outputPath, 3, 0,
          0, 0, 2, jobConf);
      mr.shutdown();

      /* Run a job with the (only)tasktracker on datanode4.
       */
      mr = new MiniMRClusterWithNodeGroup(taskTrackers, namenode, 1, rack4,
          nodeGroup4, hosts4, jobConf);

      /* The job is configured with three maps since there are three
       * (non-splittable) files. As the way in which repl was setup while
       * creating the files, we will have all the three files on datanode1 which
       * is on the same nodegroup with datanode4 where the only tasktracker run.
       * Thus, the result should be 3 nodegroup-local maps.
       * The MapReduce cluster have only 1 node which is host4 but no datanode
       * running on that host. So this is to verify that in compute/data node
       * separation case, it still can get nodegroup level locality in task
       * scheduling.
       */
      launchJobAndTestCounters(testName, mr, fileSys, inDir, outputPath, 3, 0,
          0, 3, 0, jobConf);
      mr.shutdown();
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  static RunningJob launchJob(JobConf jobConf, Path inDir, Path outputPath,
                              int numMaps, String jobName) throws IOException {
    jobConf.setJobName(jobName);
    jobConf.setInputFormat(NonSplitableSequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outputPath);
    jobConf.setMapperClass(IdentityMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);
    jobConf.setNumMapTasks(numMaps);
    jobConf.setNumReduceTasks(0);
    jobConf.setJar("build/test/testjar/testjob.jar");
    return JobClient.runJob(jobConf);
  }
}
