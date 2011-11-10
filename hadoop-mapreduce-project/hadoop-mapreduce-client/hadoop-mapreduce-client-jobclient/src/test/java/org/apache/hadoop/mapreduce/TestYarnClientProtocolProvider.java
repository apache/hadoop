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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestYarnClientProtocolProvider extends TestCase {

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
      cluster.getDelegationToken(new Text(" "));
    } finally {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

}
