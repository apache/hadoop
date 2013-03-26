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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.modes.FairSchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.modes.FifoSchedulingMode;
import org.junit.Test;

public class TestSchedulingMode {

  @Test(timeout = 1000)
  public void testParseSchedulingMode() throws AllocationConfigurationException {

    // Class name
    SchedulingMode sm = SchedulingMode
        .parse(FairSchedulingMode.class.getName());
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSchedulingMode.NAME));

    // Canonical name
    sm = SchedulingMode.parse(FairSchedulingMode.class
        .getCanonicalName());
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSchedulingMode.NAME));

    // Class
    sm = SchedulingMode.getInstance(FairSchedulingMode.class);
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSchedulingMode.NAME));

    // Shortname - fair
    sm = SchedulingMode.parse("fair");
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSchedulingMode.NAME));

    // Shortname - fifo
    sm = SchedulingMode.parse("fifo");
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FifoSchedulingMode.NAME));
  }
}
