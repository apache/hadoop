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
package org.apache.hadoop.yarn.service.monitor.probe;

import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.service.api.records.ReadinessCheck;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for default probe.
 */
public class TestDefaultProbe {
  private DefaultProbe probe;

  public void initTestDefaultProbe(Probe paramProbe) {
    this.probe = (DefaultProbe) paramProbe;
  }

  public static Collection<Object[]> data() {
    // test run 1: Default probe checks that container has an IP
    Probe p1 = MonitorUtils.getProbe(null);

    // test run 2: Default probe with DNS check for component instance hostname
    ReadinessCheck rc2 = new ReadinessCheck()
        .type(ReadinessCheck.TypeEnum.DEFAULT)
        .properties(Collections.singletonMap(
            MonitorKeys.DEFAULT_PROBE_DNS_CHECK_ENABLED, "true"));
    Probe p2 = MonitorUtils.getProbe(rc2);

    // test run 3: Default probe with DNS check using specific DNS server
    Map<String, String> props = new HashMap<>();
    props.put(MonitorKeys.DEFAULT_PROBE_DNS_CHECK_ENABLED, "true");
    props.put(MonitorKeys.DEFAULT_PROBE_DNS_ADDRESS, "8.8.8.8");
    ReadinessCheck rc3 = new ReadinessCheck()
        .type(ReadinessCheck.TypeEnum.DEFAULT).properties(props);
    Probe p3 = MonitorUtils.getProbe(rc3);

    return Arrays.asList(new Object[][] {{p1}, {p2}, {p3}});
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testDefaultProbe(Probe paramProbe) {
    initTestDefaultProbe(paramProbe);
    // component instance has a good hostname, so probe will eventually succeed
    // whether or not DNS checking is enabled
    ComponentInstance componentInstance =
        createMockComponentInstance("example.com");
    checkPingResults(probe, componentInstance, false);

    // component instance has a bad hostname, so probe will fail when DNS
    // checking is enabled
    componentInstance = createMockComponentInstance("bad.dns.test");
    checkPingResults(probe, componentInstance, probe.isDnsCheckEnabled());
  }

  private static void checkPingResults(Probe probe, ComponentInstance
      componentInstance, boolean expectDNSCheckFailure) {
    // on the first ping, null container status results in failure
    ProbeStatus probeStatus = probe.ping(componentInstance);
    assertFalse(probeStatus.isSuccess(),
        "Expected failure for " + probeStatus.toString());
    assertTrue(probeStatus.toString().contains(
        componentInstance.getCompInstanceName() + ": IP is not available yet"),
        "Expected IP failure for " + probeStatus.toString());

    // on the second ping, container status is retrieved but there are no
    // IPs, resulting in failure
    probeStatus = probe.ping(componentInstance);
    assertFalse(probeStatus.isSuccess(),
        "Expected failure for " + probeStatus.toString());
    assertTrue(probeStatus.toString().contains(componentInstance
        .getCompInstanceName() + ": IP is not available yet"),
        "Expected IP failure for " + probeStatus.toString());

    // on the third ping, IPs are retrieved and success depends on whether or
    // not a DNS lookup can be performed for the component instance hostname
    probeStatus = probe.ping(componentInstance);
    if (expectDNSCheckFailure) {
      assertFalse(probeStatus.isSuccess(),
          "Expected failure for " + probeStatus.toString());
      assertTrue(probeStatus.toString().contains(componentInstance
          .getCompInstanceName() + ": DNS checking is enabled, but lookup" +
          " for " + componentInstance.getHostname() + " is not available " +
          "yet"), "Expected DNS failure for " + probeStatus.toString());
    } else {
      assertTrue(probeStatus.isSuccess(),
          "Expected success for " + probeStatus.toString());
    }
  }

  private static ComponentInstance createMockComponentInstance(String
      hostname) {
    ComponentInstance componentInstance = mock(ComponentInstance.class);
    when(componentInstance.getHostname()).thenReturn(hostname);
    when(componentInstance.getCompInstanceName()).thenReturn("comp-0");
    when(componentInstance.getContainerStatus())
        .thenAnswer(new Answer<ContainerStatus>() {
          private int count = 0;

          @Override
          public ContainerStatus answer(InvocationOnMock invocationOnMock) {
            count++;
            if (count == 1) {
              // first call to getContainerStatus returns null
              return null;
            } else if (count == 2) {
              // second call returns a ContainerStatus with no IPs
              ContainerStatus containerStatus = mock(ContainerStatus.class);
              when(containerStatus.getIPs()).thenReturn(null);
              return containerStatus;
            } else {
              // third call returns a ContainerStatus with one IP
              ContainerStatus containerStatus = mock(ContainerStatus.class);
              when(containerStatus.getIPs())
                  .thenReturn(Collections.singletonList("1.2.3.4"));
              return containerStatus;
            }
          }
        });
    return componentInstance;
  }
}
