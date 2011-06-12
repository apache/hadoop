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

package org.apache.hadoop.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.tools.MRAdmin;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMapredGroupMappingServiceRefresh {
  private MiniDFSCluster cluster;
  JobConf config;
  private static long groupRefreshTimeoutSec = 2;
  private static final Log LOG = LogFactory
      .getLog(TestMapredGroupMappingServiceRefresh.class);
  
  public static class MockUnixGroupsMapping implements GroupMappingServiceProvider {
    private int i=0;
    
    @Override
    public List<String> getGroups(String user) throws IOException {
      String g1 = user + (10 * i + 1);
      String g2 = user + (10 * i + 2);
      List<String> l = new ArrayList<String>(2);
      l.add(g1);
      l.add(g2);
      i++;
      return l;
    }
  }
  
  @Before
  public void setUp() throws Exception {
    config = new JobConf(new Configuration());
    
    config.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        TestMapredGroupMappingServiceRefresh.MockUnixGroupsMapping.class,
        GroupMappingServiceProvider.class);
    config.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 
        groupRefreshTimeoutSec);
    
    LOG.info("GROUP MAPPING class name=" + 
        config.getClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        ShellBasedUnixGroupsMapping.class,GroupMappingServiceProvider.class).
        getName());
    
    Groups.getUserToGroupsMappingService(config);
    String namenodeUrl = "hdfs://localhost:" + "0";
    FileSystem.setDefaultUri(config, namenodeUrl);
    
    cluster = new MiniDFSCluster(0, config, 1, true, true, true,  null, null, 
        null, null);
    cluster.waitActive();
    URI uri = cluster.getURI();
    
    MiniMRCluster miniMRCluster = new MiniMRCluster(0, uri.toString() , 
      3, null, null, config);
    
    config.set(JTConfig.JT_IPC_ADDRESS, "localhost:"+miniMRCluster.getJobTrackerPort());
  }

  @After
  public void tearDown() throws Exception {
    if(cluster!=null) {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testGroupMappingRefresh() throws Exception {
    MRAdmin admin = new MRAdmin(config);
    String [] args = new String[] { "-refreshUserToGroupsMappings" };
    
    Groups groups = Groups.getUserToGroupsMappingService(config);
    String user = UserGroupInformation.getLoginUser().getShortUserName();
    System.out.println("first attempt:");
    List<String> g1 = groups.getGroups(user);
    String [] str_groups = new String [g1.size()];
    g1.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    
    System.out.println("second attempt, should be same:");
    List<String> g2 = groups.getGroups(user);
    g2.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    for(int i=0; i<g2.size(); i++) {
      assertEquals("Should be same group ", g1.get(i), g2.get(i));
    }
    // run refresh command
    admin.run(args);
    
    System.out.println("third attempt(after refresh command), should be different:");
    List<String> g3 = groups.getGroups(user);
    g3.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    for(int i=0; i<g3.size(); i++) {
      assertFalse("Should be different group ", g1.get(i).equals(g3.get(i)));
    }
    System.out.println("");
    
    // test time out
    Thread.sleep(groupRefreshTimeoutSec*1100);
    System.out.println("fourth attempt(after timeout), should be different:");
    List<String> g4 = groups.getGroups(user);
    g4.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    for(int i=0; i<g4.size(); i++) {
      assertFalse("Should be different group ", g3.get(i).equals(g4.get(i)));
    }
  }
}
