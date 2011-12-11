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
import java.io.File;
import java.io.FileOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.test.system.AbstractDaemonClient;
import org.apache.hadoop.test.system.process.HadoopDaemonRemoteCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPushConfig {
  private static MRCluster cluster;
  private String localConfDir = "localconf";
  private static final Log LOG = LogFactory.getLog(
      TestPushConfig.class.getName());
  
  @BeforeClass
  public static void before() throws Exception {
    String [] expExcludeList = new String[2];
    expExcludeList[0] = "java.net.ConnectException";
    expExcludeList[1] = "java.io.IOException";
    
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
  }

  @AfterClass
  public static void after() throws Exception {
    cluster.tearDown();
  }
  
  /**
   * This test about testing the pushConfig feature. The pushConfig functionality
   * available as part of the cluster process manager. The functionality takes
   * in local input directory and pushes all the files from the local to the 
   * remote conf directory. This functionality is required is change the config
   * on the fly and restart the cluster which will be used by other test cases
   * @throws Exception is thrown if pushConfig fails. 
   */
  @Test
  public void testPushConfig() throws Exception {
    final String DUMMY_CONFIG_STRING = "mapreduce.newdummy.conf";
    final String DUMMY_CONFIG_STRING_VALUE = "HerriotTestRules";
    Configuration origconf = new Configuration(cluster.getConf());
    origconf.set(DUMMY_CONFIG_STRING, DUMMY_CONFIG_STRING_VALUE);
    String localDir = HadoopDaemonRemoteCluster.getDeployedHadoopConfDir() + 
        File.separator + localConfDir;
    File lFile = new File(localDir);
    if(!lFile.exists()){
      lFile.mkdir();
    }
    String mapredConf = localDir + File.separator + "mapred-site.xml";
    File file = new File(mapredConf);
    origconf.writeXml(new FileOutputStream(file));    
    Configuration daemonConf =  cluster.getJTClient().getProxy().getDaemonConf();
    Assert.assertTrue("Dummy varialble is expected to be null before restart.",
        daemonConf.get(DUMMY_CONFIG_STRING) == null);
    String newDir = cluster.getClusterManager().pushConfig(localDir);
    cluster.stop();
    AbstractDaemonClient cli = cluster.getJTClient();
    waitForClusterStop(cli);
    // make sure the cluster has actually stopped
    cluster.getClusterManager().start(newDir);
    cli = cluster.getJTClient();
    waitForClusterStart(cli);
    // make sure the cluster has actually started
    Configuration newconf = cluster.getJTClient().getProxy().getDaemonConf();
    Assert.assertTrue("Extra varialble is expected to be set",
        newconf.get(DUMMY_CONFIG_STRING).equals(DUMMY_CONFIG_STRING_VALUE));
    cluster.getClusterManager().stop(newDir);
    cli = cluster.getJTClient();
    // make sure the cluster has actually stopped
    waitForClusterStop(cli);
    // start the daemons with original conf dir
    cluster.getClusterManager().start();
    cli = cluster.getJTClient();    
    waitForClusterStart(cli);  
    daemonConf =  cluster.getJTClient().getProxy().getDaemonConf();
    Assert.assertTrue("Dummy variable is expected to be null after restart.",
        daemonConf.get(DUMMY_CONFIG_STRING) == null);
    lFile.delete();
  }
  
  private void waitForClusterStop(AbstractDaemonClient cli) throws Exception {
    int i=1;
    while (i < 40) {
      try {
        cli.ping();
        Thread.sleep(1000);
        i++;
      } catch (Exception e) {
        break;
      }
    }
    for (AbstractDaemonClient tcli : cluster.getTTClients()) {
      i = 1;
      while (i < 40) {
        try {
          tcli.ping();
          Thread.sleep(1000);
          i++;
        } catch (Exception e) {
          break;
        }
      }
      if (i >= 40) {
        Assert.fail("TT on " + tcli.getHostName() + " Should have been down.");
      }
    }
  }
  
  private void waitForClusterStart(AbstractDaemonClient cli) throws Exception {
    int i=1;
    while (i < 40) {
      try {
        cli.ping();
        break;
      } catch (Exception e) {
        i++;
        Thread.sleep(1000);
        LOG.info("Waiting for Jobtracker on host : "
            + cli.getHostName() + " to come up.");
      }
    }
    for (AbstractDaemonClient tcli : cluster.getTTClients()) {
      i = 1;
      while (i < 40) {
        try {
          tcli.ping();
          break;
        } catch (Exception e) {
          i++;
          Thread.sleep(1000);
          LOG.info("Waiting for Tasktracker on host : "
              + tcli.getHostName() + " to come up.");
        }
      }
    }
  }
}
