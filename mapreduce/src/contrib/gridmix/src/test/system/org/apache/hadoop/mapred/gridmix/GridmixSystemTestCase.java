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

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapred.gridmix.test.system.GridmixJobSubmission;
import org.apache.hadoop.mapred.gridmix.test.system.GridmixJobVerification;
import org.apache.hadoop.mapred.gridmix.test.system.GridMixRunMode;
import org.apache.hadoop.mapred.gridmix.test.system.UtilsForGridmix;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Collection;
import java.io.IOException;

/**
 * Run and verify the Gridmix jobs for given a trace.
 */
public class GridmixSystemTestCase {
  private static final Log LOG = LogFactory
      .getLog(GridmixSystemTestCase.class);
  public static Configuration  conf = new Configuration();
  public static MRCluster cluster;
  public static int cSize;
  public static JTClient jtClient;
  public static JTProtocol rtClient;
  public static Path gridmixDir;
  public static Map<String, String> map;
  private static GridmixJobSubmission gridmixJS;
  public static GridmixJobVerification gridmixJV;
  public static List<JobID> jobids;
  
  @BeforeClass
  public static void before() throws Exception {
    String [] excludeExpList = {"java.net.ConnectException", 
       "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(excludeExpList);
    cluster.setUp();
    cSize = cluster.getTTClients().size();
    jtClient = cluster.getJTClient();
    rtClient = jtClient.getProxy();
    gridmixDir = new Path("herriot-gridmix");
    UtilsForGridmix.createDirs(gridmixDir, rtClient.getDaemonConf());
    map = UtilsForGridmix.getMRTraces(rtClient.getDaemonConf());
  }

  @AfterClass
  public static void after() throws Exception {
    UtilsForGridmix.cleanup(gridmixDir, rtClient.getDaemonConf());
    org.apache.hadoop.fs.FileUtil.fullyDelete(new java.io.File("/tmp/gridmix-st"));
    cluster.tearDown();
    if (gridmixJS.getJobConf().get("gridmix.user.resolve.class").
        contains("RoundRobin")) {
       List<String> proxyUsers = UtilsForGridmix.
           listProxyUsers(gridmixJS.getJobConf(),
           UserGroupInformation.getLoginUser().getShortUserName());
       for(int index = 0; index < proxyUsers.size(); index++){
         UtilsForGridmix.cleanup(new Path("hdfs:///user/" + 
            proxyUsers.get(index)), 
            rtClient.getDaemonConf());
       }
    }
  }
  
  public static void runGridmixAndVerify(String[] runtimeValues, 
     String [] otherValues, String tracePath) throws Exception {
     runGridmixAndVerify(runtimeValues, otherValues, tracePath , 
         GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX);
  }
  
  public static void runGridmixAndVerify(String [] runtimeValues, 
      String [] otherValues, String tracePath, int mode) throws Exception {
    jobids = runGridmix(runtimeValues, otherValues, mode);
    gridmixJV = new GridmixJobVerification(
        new Path(tracePath), gridmixJS.getJobConf(), jtClient);
    gridmixJV.verifyGridmixJobsWithJobStories(jobids);  
  }
  
  public static List<JobID> runGridmix(String[] runtimeValues, 
     String[] otherValues, int mode) throws Exception {
    gridmixJS = new GridmixJobSubmission(rtClient.getDaemonConf(),
       jtClient, gridmixDir);
    gridmixJS.submitJobs(runtimeValues, otherValues, mode);
    jobids = UtilsForGridmix.listGridmixJobIDs(jtClient.getClient(), 
       gridmixJS.getGridmixJobCount());
    return jobids;
  }
  
  
  public static String getTraceFile(String regExp) throws IOException {
    List<String> listTraces = UtilsForGridmix.listMRTraces(
        rtClient.getDaemonConf());
    Iterator<String> ite = listTraces.iterator();
    while(ite.hasNext()) {
      String traceFile = ite.next();
      if (traceFile.indexOf(regExp)>=0) {
        return traceFile;
      }
    }
    return null;
  }
}
