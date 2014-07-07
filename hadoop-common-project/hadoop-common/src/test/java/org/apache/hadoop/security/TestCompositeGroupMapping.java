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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;


public class TestCompositeGroupMapping {
  public static final Log LOG = LogFactory.getLog(TestCompositeGroupMapping.class);
  private static Configuration conf = new Configuration();
  
  private static class TestUser {
    String name;
    String group;
    String group2;
    
    public TestUser(String name, String group) {
      this.name = name;
      this.group = group;
    }

    public TestUser(String name, String group, String group2) {
      this(name, group);
      this.group2 = group2;
    }
  };
  
  private static TestUser john = new TestUser("John", "user-group");
  private static TestUser hdfs = new TestUser("hdfs", "supergroup");
  private static TestUser jack = new TestUser("Jack", "user-group", "dev-group-1");
  
  private static final String PROVIDER_SPECIFIC_CONF = ".test.prop";
  private static final String PROVIDER_SPECIFIC_CONF_KEY = 
      GroupMappingServiceProvider.GROUP_MAPPING_CONFIG_PREFIX + PROVIDER_SPECIFIC_CONF;
  private static final String PROVIDER_SPECIFIC_CONF_VALUE_FOR_USER = "value-for-user";
  private static final String PROVIDER_SPECIFIC_CONF_VALUE_FOR_CLUSTER = "value-for-cluster";
  
  private static abstract class GroupMappingProviderBase 
    implements GroupMappingServiceProvider, Configurable {
    
    private Configuration conf;
    
    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return this.conf;
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
      
    }
    
    protected List<String> toList(String group) {
      if (group != null) {
        return Arrays.asList(new String[] {group});
      }
      return new ArrayList<String>();
    }
    
    protected void checkTestConf(String expectedValue) {
      String configValue = getConf().get(PROVIDER_SPECIFIC_CONF_KEY);
      if (configValue == null || !configValue.equals(expectedValue)) {
        throw new RuntimeException("Failed to find mandatory configuration of " + PROVIDER_SPECIFIC_CONF_KEY);
      }
    }
  };
  
  private static class UserProvider extends GroupMappingProviderBase {
    @Override
    public List<String> getGroups(String user) throws IOException {
      checkTestConf(PROVIDER_SPECIFIC_CONF_VALUE_FOR_USER);
      
      String group = null;
      if (user.equals(john.name)) {
        group = john.group;
      } else if (user.equals(jack.name)) {
        group = jack.group;
      }
      
      return toList(group);
    }
  }
  
  private static class ClusterProvider extends GroupMappingProviderBase {    
    @Override
    public List<String> getGroups(String user) throws IOException {
      checkTestConf(PROVIDER_SPECIFIC_CONF_VALUE_FOR_CLUSTER);
      
      String group = null;
      if (user.equals(hdfs.name)) {
        group = hdfs.group;
      } else if (user.equals(jack.name)) { // jack has another group from clusterProvider
        group = jack.group2;
      }
      
      return toList(group);
    }
  }
  
  static {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
      CompositeGroupsMapping.class, GroupMappingServiceProvider.class);
    conf.set(CompositeGroupsMapping.MAPPING_PROVIDERS_CONFIG_KEY, "userProvider,clusterProvider");

    conf.setClass(CompositeGroupsMapping.MAPPING_PROVIDER_CONFIG_PREFIX + ".userProvider", 
        UserProvider.class, GroupMappingServiceProvider.class);

    conf.setClass(CompositeGroupsMapping.MAPPING_PROVIDER_CONFIG_PREFIX + ".clusterProvider", 
        ClusterProvider.class, GroupMappingServiceProvider.class);

    conf.set(CompositeGroupsMapping.MAPPING_PROVIDER_CONFIG_PREFIX + 
        ".clusterProvider" + PROVIDER_SPECIFIC_CONF, PROVIDER_SPECIFIC_CONF_VALUE_FOR_CLUSTER);

    conf.set(CompositeGroupsMapping.MAPPING_PROVIDER_CONFIG_PREFIX + 
        ".userProvider" + PROVIDER_SPECIFIC_CONF, PROVIDER_SPECIFIC_CONF_VALUE_FOR_USER);
  }

  @Test
  public void TestMultipleGroupsMapping() throws Exception {
    Groups groups = new Groups(conf);

    assertTrue(groups.getGroups(john.name).get(0).equals(john.group));
    assertTrue(groups.getGroups(hdfs.name).get(0).equals(hdfs.group));
  }

  @Test
  public void TestMultipleGroupsMappingWithCombined() throws Exception {
    conf.set(CompositeGroupsMapping.MAPPING_PROVIDERS_COMBINED_CONFIG_KEY, "true");
    Groups groups = new Groups(conf);

    assertTrue(groups.getGroups(jack.name).size() == 2);
    // the configured providers list in order is "userProvider,clusterProvider"
    // group -> userProvider, group2 -> clusterProvider
    assertTrue(groups.getGroups(jack.name).contains(jack.group));
    assertTrue(groups.getGroups(jack.name).contains(jack.group2));
  }

  @Test
  public void TestMultipleGroupsMappingWithoutCombined() throws Exception {
    conf.set(CompositeGroupsMapping.MAPPING_PROVIDERS_COMBINED_CONFIG_KEY, "false");
    Groups groups = new Groups(conf);

    // the configured providers list in order is "userProvider,clusterProvider"
    // group -> userProvider, group2 -> clusterProvider
    assertTrue(groups.getGroups(jack.name).size() == 1);
    assertTrue(groups.getGroups(jack.name).get(0).equals(jack.group));
  }
}
