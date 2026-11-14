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

package org.apache.hadoop.yarn.server.federation.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.ConnectException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link FederationApplicationRMProbe}.
 */
public class TestFederationApplicationRMProbe {

  private static final long TIMEOUT_MS = 1000;

  private final ApplicationId appId = ApplicationId.newInstance(1, 1);
  private final SubClusterId sc1 = SubClusterId.newInstance("SC1");
  private final SubClusterId sc2 = SubClusterId.newInstance("SC2");
  private final SubClusterId sc3 = SubClusterId.newInstance("SC3");
  private final Map<SubClusterId, ApplicationClientProtocol> rms =
      new HashMap<>();
  private final FederationApplicationRMProbe probe =
      new FederationApplicationRMProbe(rms::get, TIMEOUT_MS);

  @AfterEach
  public void tearDown() {
    probe.close();
  }

  private enum Behavior { KNOWS, UNKNOWN, DOWN, HANGS }

  private void rm(SubClusterId subClusterId, Behavior behavior)
      throws Exception {
    ApplicationClientProtocol rm = mock(ApplicationClientProtocol.class);
    switch (behavior) {
    case KNOWS:
      ApplicationReport report = Records.newRecord(ApplicationReport.class);
      report.setApplicationId(appId);
      when(rm.getApplicationReport(any(GetApplicationReportRequest.class)))
          .thenReturn(GetApplicationReportResponse.newInstance(report));
      break;
    case UNKNOWN:
      when(rm.getApplicationReport(any(GetApplicationReportRequest.class)))
          .thenThrow(new ApplicationNotFoundException("not here"));
      break;
    case DOWN:
      when(rm.getApplicationReport(any(GetApplicationReportRequest.class)))
          .thenThrow(new ConnectException("down"));
      break;
    case HANGS:
      when(rm.getApplicationReport(any(GetApplicationReportRequest.class)))
          .thenAnswer(invocation -> {
            Thread.sleep(60000);
            return null;
          });
      break;
    default:
      throw new IllegalArgumentException(behavior.toString());
    }
    rms.put(subClusterId, rm);
  }

  private void assertUnconfirmed(FederationApplicationRMProbe p,
      List<SubClusterId> subClusters) {
    YarnException e = assertThrows(YarnException.class,
        () -> p.find(appId, subClusters),
        "An unconfirmed lookup must not look like not found");
    assertTrue(e.getMessage().startsWith("Unable to ask every SubCluster"),
        e.getMessage());
  }

  @Test
  public void testFindsTheRMThatKnowsTheApplication() throws Exception {
    rm(sc1, Behavior.UNKNOWN);
    rm(sc2, Behavior.DOWN);
    rm(sc3, Behavior.KNOWS);
    assertEquals(sc3, probe.find(appId, Arrays.asList(sc1, sc2, sc3)));
  }

  @Test
  public void testNoRMKnowsTheApplication() throws Exception {
    rm(sc1, Behavior.UNKNOWN);
    rm(sc2, Behavior.UNKNOWN);
    assertNull(probe.find(appId, Arrays.asList(sc1, sc2)));
  }

  /** An RM that cannot be asked might know the application. */
  @Test
  public void testUnreachableRMIsNotNotFound() throws Exception {
    rm(sc1, Behavior.UNKNOWN);
    rm(sc2, Behavior.DOWN);
    assertUnconfirmed(probe, Arrays.asList(sc1, sc2));
  }

  /** An RM that cannot be asked because the others fill the pool. */
  @Test
  public void testSaturatedProbeIsNotNotFound() throws Exception {
    rm(sc1, Behavior.HANGS);
    rm(sc2, Behavior.UNKNOWN);
    FederationApplicationRMProbe small =
        new FederationApplicationRMProbe(rms::get, TIMEOUT_MS, 1);
    try {
      Thread busy = new Thread(() -> {
        try {
          small.find(appId, Arrays.asList(sc1));
        } catch (YarnException e) {
          // Expected once the timeout is over
        }
      });
      busy.start();
      Thread.sleep(200);
      long start = System.currentTimeMillis();
      assertUnconfirmed(small, Arrays.asList(sc2));
      assertTrue(System.currentTimeMillis() - start < TIMEOUT_MS);
      busy.join();
    } finally {
      small.close();
    }
  }

  @Test
  public void testHangingRMDoesNotDelayTheAnswer() throws Exception {
    rm(sc1, Behavior.HANGS);
    rm(sc2, Behavior.KNOWS);
    long start = System.currentTimeMillis();
    assertEquals(sc2, probe.find(appId, Arrays.asList(sc1, sc2)));
    assertTrue(System.currentTimeMillis() - start < TIMEOUT_MS);
  }

  @Test
  public void testHangingRMIsBoundedByTheTimeout() throws Exception {
    rm(sc1, Behavior.HANGS);
    rm(sc2, Behavior.UNKNOWN);
    long start = System.currentTimeMillis();
    assertUnconfirmed(probe, Arrays.asList(sc1, sc2));
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed >= TIMEOUT_MS - 100 && elapsed < TIMEOUT_MS + 2000,
        "Took " + elapsed + " ms");
  }

  @Test
  public void testProbeConf() {
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.ROUTER_APPLICATION_PROBE_TIMEOUT, "5s");
    Configuration probeConf = FederationApplicationRMProbe.createProbeConf(conf);
    assertEquals(5000, FederationApplicationRMProbe.getTimeout(conf));
    assertEquals(5000,
        probeConf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 0));
    assertEquals(0, probeConf.getLong(
        YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, -1));
  }
}
