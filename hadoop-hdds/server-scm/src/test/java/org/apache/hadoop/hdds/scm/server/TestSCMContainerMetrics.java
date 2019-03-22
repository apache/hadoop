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
package org.apache.hadoop.hdds.scm.server;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.junit.Test;

/**
 * Test metrics that represent container states.
 */
public class TestSCMContainerMetrics {
  @Test
  public void testSCMContainerMetrics() {
    SCMMXBean scmmxBean = mock(SCMMXBean.class);

    Map<String, Integer> stateInfo = new HashMap<String, Integer>() {{
        put(HddsProtos.LifeCycleState.OPEN.toString(), 2);
        put(HddsProtos.LifeCycleState.CLOSING.toString(), 3);
        put(HddsProtos.LifeCycleState.QUASI_CLOSED.toString(), 4);
        put(HddsProtos.LifeCycleState.CLOSED.toString(), 5);
        put(HddsProtos.LifeCycleState.DELETING.toString(), 6);
        put(HddsProtos.LifeCycleState.DELETED.toString(), 7);
      }};


    when(scmmxBean.getContainerStateCount()).thenReturn(stateInfo);

    MetricsRecordBuilder mb = mock(MetricsRecordBuilder.class);
    when(mb.addGauge(any(MetricsInfo.class), anyInt())).thenReturn(mb);

    MetricsCollector metricsCollector = mock(MetricsCollector.class);
    when(metricsCollector.addRecord(anyString())).thenReturn(mb);

    SCMContainerMetrics containerMetrics = new SCMContainerMetrics(scmmxBean);

    containerMetrics.getMetrics(metricsCollector, true);

    verify(mb, times(1)).addGauge(Interns.info("OpenContainers",
        "Number of open containers"), 2);
    verify(mb, times(1)).addGauge(Interns.info("ClosingContainers",
        "Number of containers in closing state"), 3);
    verify(mb, times(1)).addGauge(Interns.info("QuasiClosedContainers",
        "Number of containers in quasi closed state"), 4);
    verify(mb, times(1)).addGauge(Interns.info("ClosedContainers",
        "Number of containers in closed state"), 5);
    verify(mb, times(1)).addGauge(Interns.info("DeletingContainers",
        "Number of containers in deleting state"), 6);
    verify(mb, times(1)).addGauge(Interns.info("DeletedContainers",
        "Number of containers in deleted state"), 7);
  }
}
