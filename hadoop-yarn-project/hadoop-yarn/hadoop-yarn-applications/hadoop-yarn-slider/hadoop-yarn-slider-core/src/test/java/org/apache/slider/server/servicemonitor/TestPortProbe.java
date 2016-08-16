/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.server.servicemonitor;

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestPortProbe extends Assert {
  /**
   * Assert that a port probe failed if the port is closed
   * @throws Throwable
   */
  @Test
  public void testPortProbeFailsClosedPort() throws Throwable {
    PortProbe probe = new PortProbe("127.0.0.1", 65500, 100, "", new Configuration());
    probe.init();
    ProbeStatus status = probe.ping(true);
    assertFalse("Expected a failure but got successful result: " + status,
      status.isSuccess());
  }
}
