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

package org.apache.hadoop.mapreduce;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;

public class TestYarnClientProtocolProvider extends TestCase {
  
  private static final RecordFactory recordFactory = RecordFactoryProvider.
      getRecordFactory(null);
  
  @Test
  public void testClusterWithYarnClientProtocolProvider() throws Exception {

    Configuration conf = new Configuration(false);
    Cluster cluster = null;

    try {
      cluster = new Cluster(conf);
      fail("Cluster should not be initialized with out any framework name");
    } catch (IOException e) {

    }

    try {
      conf = new Configuration();
      conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
      cluster = new Cluster(conf);
      ClientProtocol client = cluster.getClient();
      assertTrue(client instanceof YARNRunner);
    } catch (IOException e) {

    } finally {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

 
  @Test
  public void testClusterGetDelegationToken() throws Exception {

    Configuration conf = new Configuration(false);
    Cluster cluster = null;
    try {
      conf = new Configuration();
      conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
      cluster = new Cluster(conf);
      YARNRunner yrunner = (YARNRunner) cluster.getClient();
      GetDelegationTokenResponse getDTResponse = 
          recordFactory.newRecordInstance(GetDelegationTokenResponse.class);
      DelegationToken rmDTToken = recordFactory.newRecordInstance(
          DelegationToken.class);
      rmDTToken.setIdentifier(ByteBuffer.wrap(new byte[2]));
      rmDTToken.setKind("Testclusterkind");
      rmDTToken.setPassword(ByteBuffer.wrap("testcluster".getBytes()));
      rmDTToken.setService("0.0.0.0:8032");
      getDTResponse.setRMDelegationToken(rmDTToken);
      ClientRMProtocol cRMProtocol = mock(ClientRMProtocol.class);
      when(cRMProtocol.getDelegationToken(any(
          GetDelegationTokenRequest.class))).thenReturn(getDTResponse);
      ResourceMgrDelegate rmgrDelegate = new ResourceMgrDelegate(
          new YarnConfiguration(conf), cRMProtocol);
      yrunner.setResourceMgrDelegate(rmgrDelegate);
      Token t = cluster.getDelegationToken(new Text(" "));
      assertTrue("Testclusterkind".equals(t.getKind().toString()));
    } finally {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

}
