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

package org.apache.hadoop.ipc;
import static org.junit.Assert.*;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.TestConnectionRetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.junit.Before;
import org.junit.Test;

/**
 * This class mainly tests behaviors of reusing RPC connections for various
 * retry policies.
 */
public class TestReuseRpcConnections extends TestRpcBase {
  @Before
  public void setup() {
    setupConf();
  }

  private static RetryPolicy getDefaultRetryPolicy(
      final boolean defaultRetryPolicyEnabled,
      final String defaultRetryPolicySpec) {
    return TestConnectionRetryPolicy.getDefaultRetryPolicy(
        conf,
        defaultRetryPolicyEnabled,
        defaultRetryPolicySpec,
        "");
  }

  private static RetryPolicy getDefaultRetryPolicy(
      final boolean defaultRetryPolicyEnabled,
      final String defaultRetryPolicySpec,
      final String remoteExceptionToRetry) {
    return TestConnectionRetryPolicy.getDefaultRetryPolicy(
        conf,
        defaultRetryPolicyEnabled,
        defaultRetryPolicySpec,
        remoteExceptionToRetry);
  }

  @Test(timeout = 60000)
  public void testDefaultRetryPolicyReuseConnections() throws Exception {
    RetryPolicy rp1 = null;
    RetryPolicy rp2 = null;
    RetryPolicy rp3 = null;

    /* test the same setting */
    rp1 = getDefaultRetryPolicy(true, "10000,2");
    rp2 = getDefaultRetryPolicy(true, "10000,2");
    verifyRetryPolicyReuseConnections(rp1, rp2, RetryPolicies.RETRY_FOREVER);

    /* test enabled and different specifications */
    rp1 = getDefaultRetryPolicy(true, "20000,3");
    rp2 = getDefaultRetryPolicy(true, "20000,3");
    rp3 = getDefaultRetryPolicy(true, "30000,4");
    verifyRetryPolicyReuseConnections(rp1, rp2, rp3);

    /* test disabled and the same specifications */
    rp1 = getDefaultRetryPolicy(false, "40000,5");
    rp2 = getDefaultRetryPolicy(false, "40000,5");
    verifyRetryPolicyReuseConnections(rp1, rp2, RetryPolicies.RETRY_FOREVER);

    /* test disabled and different specifications */
    rp1 = getDefaultRetryPolicy(false, "50000,6");
    rp2 = getDefaultRetryPolicy(false, "60000,7");
    verifyRetryPolicyReuseConnections(rp1, rp2, RetryPolicies.RETRY_FOREVER);

    /* test different remoteExceptionToRetry */
    rp1 = getDefaultRetryPolicy(
        true,
        "70000,8",
        new RemoteException(
            RpcNoSuchMethodException.class.getName(),
            "no such method exception").getClassName());
    rp2 = getDefaultRetryPolicy(
        true,
        "70000,8",
        new RemoteException(
            PathIOException.class.getName(),
            "path IO exception").getClassName());
    verifyRetryPolicyReuseConnections(rp1, rp2, RetryPolicies.RETRY_FOREVER);
  }

  @Test(timeout = 60000)
  public void testRetryPolicyTryOnceThenFail() throws Exception {
    final RetryPolicy rp1 = TestConnectionRetryPolicy.newTryOnceThenFail();
    final RetryPolicy rp2 = TestConnectionRetryPolicy.newTryOnceThenFail();
    verifyRetryPolicyReuseConnections(rp1, rp2, RetryPolicies.RETRY_FOREVER);
  }

  private void verifyRetryPolicyReuseConnections(
      final RetryPolicy retryPolicy1,
      final RetryPolicy retryPolicy2,
      final RetryPolicy anotherRetryPolicy) throws Exception {
    final Server server = setupTestServer(conf, 2);
    final Configuration newConf = new Configuration(conf);
    newConf.set(
        CommonConfigurationKeysPublic
          .HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
        "");
    Client client = null;
    TestRpcService proxy1 = null;
    TestRpcService proxy2 = null;
    TestRpcService proxy3 = null;

    try {
      proxy1 = getClient(addr, newConf, retryPolicy1);
      proxy1.ping(null, newEmptyRequest());
      client = ProtobufRpcEngine2.getClient(newConf);
      final Set<ConnectionId> conns = client.getConnectionIds();
      assertEquals("number of connections in cache is wrong", 1, conns.size());

      /*
       * another equivalent retry policy, reuse connection
       */
      proxy2 = getClient(addr, newConf, retryPolicy2);
      proxy2.ping(null, newEmptyRequest());
      assertEquals("number of connections in cache is wrong", 1, conns.size());

      /*
       * different retry policy, create a new connection
       */
      proxy3 = getClient(addr, newConf, anotherRetryPolicy);
      proxy3.ping(null, newEmptyRequest());
      assertEquals("number of connections in cache is wrong", 2, conns.size());
    } finally {
      server.stop();
      // this is dirty, but clear out connection cache for next run
      if (client != null) {
        client.getConnectionIds().clear();
      }
      if (proxy1 != null) {
        RPC.stopProxy(proxy1);
      }
      if (proxy2 != null) {
        RPC.stopProxy(proxy2);
      }
      if (proxy3 != null) {
        RPC.stopProxy(proxy3);
      }
    }
  }
}
