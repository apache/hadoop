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

package org.apache.hadoop.io.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.RpcNoSuchMethodException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class mainly tests behaviors of various retry policies in connection
 * level.
 */
public class TestConnectionRetryPolicy {
  private static RetryPolicy getDefaultRetryPolicy(
      final boolean defaultRetryPolicyEnabled,
      final String defaultRetryPolicySpec,
      final String remoteExceptionToRetry) {
    return getDefaultRetryPolicy(
        new Configuration(),
        defaultRetryPolicyEnabled,
        defaultRetryPolicySpec,
        remoteExceptionToRetry);
  }

  private static RetryPolicy getDefaultRetryPolicy(
      final boolean defaultRetryPolicyEnabled,
      final String defaultRetryPolicySpec) {
    return getDefaultRetryPolicy(
        new Configuration(),
        defaultRetryPolicyEnabled,
        defaultRetryPolicySpec,
        "");
  }

  public static RetryPolicy getDefaultRetryPolicy(
      final Configuration conf,
      final boolean defaultRetryPolicyEnabled,
      final String defaultRetryPolicySpec,
      final String remoteExceptionToRetry) {
    return RetryUtils.getDefaultRetryPolicy(
        conf,
        "org.apache.hadoop.io.retry.TestConnectionRetryPolicy.No.Such.Key",
        defaultRetryPolicyEnabled,
        "org.apache.hadoop.io.retry.TestConnectionRetryPolicy.No.Such.Key",
        defaultRetryPolicySpec,
        "");
  }

  @Test
  @Timeout(value = 60)
  public void testDefaultRetryPolicyEquivalence() {
    RetryPolicy rp1 = null;
    RetryPolicy rp2 = null;
    RetryPolicy rp3 = null;

    /* test the same setting */
    rp1 = getDefaultRetryPolicy(true, "10000,2");
    rp2 = getDefaultRetryPolicy(true, "10000,2");
    rp3 = getDefaultRetryPolicy(true, "10000,2");
    verifyRetryPolicyEquivalence(new RetryPolicy[] {rp1, rp2, rp3});

    /* test different remoteExceptionToRetry */
    rp1 = getDefaultRetryPolicy(
        true,
        "10000,2",
        new RemoteException(
            PathIOException.class.getName(),
            "path IO exception").getClassName());
    rp2 = getDefaultRetryPolicy(
        true,
        "10000,2",
        new RemoteException(
            RpcNoSuchMethodException.class.getName(),
            "no such method exception").getClassName());
    rp3 = getDefaultRetryPolicy(
        true,
        "10000,2",
        new RemoteException(
            RetriableException.class.getName(),
            "retriable exception").getClassName());
    verifyRetryPolicyEquivalence(new RetryPolicy[] {rp1, rp2, rp3});

    /* test enabled and different specifications */
    rp1 = getDefaultRetryPolicy(true, "20000,3");
    rp2 = getDefaultRetryPolicy(true, "30000,4");
    assertNotEquals(rp1, rp2, "should not be equal");
    assertNotEquals(rp1.hashCode(), rp2.hashCode(),
        "should not have the same hash code");

    /* test disabled and the same specifications */
    rp1 = getDefaultRetryPolicy(false, "40000,5");
    rp2 = getDefaultRetryPolicy(false, "40000,5");
    assertEquals(rp1, rp2, "should be equal");
    assertEquals(rp1, rp2, "should have the same hash code");

    /* test the disabled and different specifications */
    rp1 = getDefaultRetryPolicy(false, "50000,6");
    rp2 = getDefaultRetryPolicy(false, "60000,7");
    assertEquals(rp1, rp2, "should be equal");
    assertEquals(rp1, rp2, "should have the same hash code");
  }

  public static RetryPolicy newTryOnceThenFail() {
    return new RetryPolicies.TryOnceThenFail();
  }

  @Test
  @Timeout(value = 60)
  public void testTryOnceThenFailEquivalence() throws Exception {
    final RetryPolicy rp1 = newTryOnceThenFail();
    final RetryPolicy rp2 = newTryOnceThenFail();
    final RetryPolicy rp3 = newTryOnceThenFail();
    verifyRetryPolicyEquivalence(new RetryPolicy[] {rp1, rp2, rp3});
  }

  private void verifyRetryPolicyEquivalence(RetryPolicy[] polices) {
    for (int i = 0; i < polices.length; i++) {
      for (int j = 0; j < polices.length; j++) {
        if (i != j) {
          assertEquals(polices[i], polices[j], "should be equal");
          assertEquals(polices[i].hashCode(),
              polices[j].hashCode(), "should have the same hash code");
        }
      }
    }
  }
}
