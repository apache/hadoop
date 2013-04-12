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

import static org.junit.Assert.*;

import java.net.InetSocketAddress;

import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

public class TestMaster {

  @Test
  public void testGetMasterAddress() {
    YarnConfiguration conf = new YarnConfiguration();

    // Default is yarn framework
    String masterHostname = Master.getMasterAddress(conf).getHostName();
    
    // no address set so should default to default rm address
    InetSocketAddress rmAddr = NetUtils.createSocketAddr(YarnConfiguration.DEFAULT_RM_ADDRESS);
    assertEquals(masterHostname, rmAddr.getHostName());
    
    // Trying invalid master address for classic 
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.CLASSIC_FRAMEWORK_NAME);
    conf.set(MRConfig.MASTER_ADDRESS, "local:invalid");

    // should throw an exception for invalid value
    try {
      Master.getMasterAddress(conf);
      fail("Should not reach here as there is a bad master address");
    }
    catch (Exception e) {
      // Expected
    }

    // Change master address to a valid value
    conf.set(MRConfig.MASTER_ADDRESS, "bar.com:8042");    
    masterHostname = Master.getMasterAddress(conf).getHostName();
    assertEquals(masterHostname, "bar.com");

    // change framework to yarn
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    conf.set(YarnConfiguration.RM_ADDRESS, "foo1.com:8192");
    masterHostname = Master.getMasterAddress(conf).getHostName();
    assertEquals(masterHostname, "foo1.com");

  }

  @Test 
  public void testGetMasterUser() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRConfig.MASTER_USER_NAME, "foo");
    conf.set(YarnConfiguration.RM_PRINCIPAL, "bar");

    // default is yarn framework  
    assertEquals(Master.getMasterUserName(conf), "bar");

    // set framework name to classic
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.CLASSIC_FRAMEWORK_NAME);
    assertEquals(Master.getMasterUserName(conf), "foo");

    // change framework to yarn
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    assertEquals(Master.getMasterUserName(conf), "bar");

  }

}
