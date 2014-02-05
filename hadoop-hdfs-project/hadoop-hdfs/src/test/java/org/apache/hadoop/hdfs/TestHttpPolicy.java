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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.http.HttpConfig.Policy.HTTP_AND_HTTPS;
import static org.apache.hadoop.http.HttpConfig.Policy.HTTP_ONLY;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public final class TestHttpPolicy {

  @Test(expected = HadoopIllegalArgumentException.class)
  public void testInvalidPolicyValue() {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, "invalid");
    DFSUtil.getHttpPolicy(conf);
  }

  @Test
  public void testDeprecatedConfiguration() {
    Configuration conf = new Configuration(false);
    Assert.assertSame(HTTP_ONLY, DFSUtil.getHttpPolicy(conf));

    conf.setBoolean(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY, true);
    Assert.assertSame(HTTP_AND_HTTPS, DFSUtil.getHttpPolicy(conf));

    conf = new Configuration(false);
    conf.setBoolean(DFSConfigKeys.HADOOP_SSL_ENABLED_KEY, true);
    Assert.assertSame(HTTP_AND_HTTPS, DFSUtil.getHttpPolicy(conf));

    conf = new Configuration(false);
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HTTP_ONLY.name());
    conf.setBoolean(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY, true);
    Assert.assertSame(HTTP_ONLY, DFSUtil.getHttpPolicy(conf));
  }
}
