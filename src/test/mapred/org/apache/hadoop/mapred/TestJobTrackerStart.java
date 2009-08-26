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
package org.apache.hadoop.mapred;

import org.apache.hadoop.mapred.UtilsForTests.FakeClock;

import junit.framework.TestCase;

/**
 * Test {@link JobTracker} w.r.t config parameters.
 */
public class TestJobTrackerStart extends TestCase {
  
  public void testJobTrackerStartConfig() throws Exception {
    JobConf conf = new JobConf();
    conf = MiniMRCluster.configureJobConf(conf, "file:///", 0, 0, null);
    
    // test JobTracker's default clock
    Clock c = JobTracker.getClock();
    assertNotNull(c);
    assertEquals(c, JobTracker.DEFAULT_CLOCK);
    
    // test with default values
    JobTracker jt = JobTracker.startTracker(conf);
    c = JobTracker.getClock();
    // test clock
    assertNotNull(c);
    assertEquals(c, JobTracker.DEFAULT_CLOCK);
    // test identifier
    assertEquals(12, jt.getTrackerIdentifier().length()); // correct upto mins
    jt.stopTracker();
    
    // test with special clock
    FakeClock myClock = new FakeClock();
    jt = JobTracker.startTracker(conf, myClock);
    c = JobTracker.getClock();
    assertNotNull(c);
    assertEquals(c, myClock);
    jt.stopTracker();
    
    // test with special identifier
    String identifier = "test-identifier";
    jt = JobTracker.startTracker(conf, JobTracker.DEFAULT_CLOCK, identifier);
    assertEquals(identifier, jt.getTrackerIdentifier());
    jt.stopTracker();
  }
}
