/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.util;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP;
import static org.junit.Assert.fail;

/**
 * Class used to test OzoneS3Util.
 */
public class TestOzoneS3Util {


  private OzoneConfiguration configuration;
  private String serviceID = "omService";

  @Before
  public void setConf() {
    configuration = new OzoneConfiguration();

    String nodeIDs = "om1,om2,om3";
    configuration.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, serviceID);
    configuration.set(OMConfigKeys.OZONE_OM_NODES_KEY + "." + serviceID,
        nodeIDs);
    configuration.setBoolean(HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, false);
  }

  @Test
  public void testBuildServiceNameForToken() {

    Collection<String> nodeIDList = OmUtils.getOMNodeIds(configuration,
        serviceID);

    configuration.set(OmUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
        serviceID, "om1"), "om1:9862");
    configuration.set(OmUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
        serviceID, "om2"), "om2:9862");
    configuration.set(OmUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
        serviceID, "om3"), "om3:9862");

    String expectedOmServiceAddress = buildServiceAddress(nodeIDList);

    SecurityUtil.setConfiguration(configuration);
    String omserviceAddr = OzoneS3Util.buildServiceNameForToken(configuration,
        serviceID, nodeIDList);

    Assert.assertEquals(expectedOmServiceAddress, omserviceAddr);
  }


  @Test
  public void testBuildServiceNameForTokenIncorrectConfig() {

    Collection<String> nodeIDList = OmUtils.getOMNodeIds(configuration,
        serviceID);

    // Don't set om3 node rpc address. Here we are skipping setting of one of
    // the OM address. So buildServiceNameForToken will fail.
    configuration.set(OmUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
        serviceID, "om1"), "om1:9862");
    configuration.set(OmUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
        serviceID, "om2"), "om2:9862");


    SecurityUtil.setConfiguration(configuration);

    try {
      OzoneS3Util.buildServiceNameForToken(configuration,
          serviceID, nodeIDList);
      fail("testBuildServiceNameForTokenIncorrectConfig failed");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("Could not find rpcAddress " +
          "for", ex);
    }


  }

  /**
   * Build serviceName from list of node ids.
   * @param nodeIDList
   * @return service name for token.
   */
  private String buildServiceAddress(Collection<String> nodeIDList) {
    StringBuilder omServiceAddrBuilder = new StringBuilder();
    int nodesLength = nodeIDList.size();
    int counter = 0;
    for (String nodeID : nodeIDList) {
      counter++;
      String addr = configuration.get(OmUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_ADDRESS_KEY, serviceID, nodeID));

      if (counter != nodesLength) {
        omServiceAddrBuilder.append(addr + ",");
      } else {
        omServiceAddrBuilder.append(addr);
      }
    }

    return omServiceAddrBuilder.toString();
  }

}
