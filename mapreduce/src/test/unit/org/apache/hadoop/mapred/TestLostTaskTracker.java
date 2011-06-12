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

import static org.mockito.Mockito.*;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.hamcrest.Matcher;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

/**
 * Tests that trackers that don't heartbeat within a given time are considered
 * lost. Note that this test is not a direct replacement for
 * {@link TestLostTracker} since it doesn't test that a task
 * running on a lost tracker is retried on another tracker.
 */
@SuppressWarnings("deprecation")
public class TestLostTaskTracker extends TestCase {

  private JobTracker jobTracker;
 
  private FakeClock clock;
  
  @Override
  protected void setUp() throws Exception {
    JobConf conf = new JobConf();
    conf.set(JTConfig.JT_IPC_ADDRESS, "localhost:0");
    conf.set(JTConfig.JT_HTTP_ADDRESS, "0.0.0.0:0");
    conf.setLong(JTConfig.JT_TRACKER_EXPIRY_INTERVAL, 1000);
    clock = new FakeClock();
    // We use a "partial mock" of JobTracker which lets us see when certain
    // methods are called. If we were writing JobTracker from scratch then
    // we would make it call another object which we would mock out instead
    // (and use a real JobTracker) so we could perform assertions on the mock.
    // See http://mockito.googlecode.com/svn/branches/1.8.0/javadoc/org/mockito/Mockito.html#16
    jobTracker = spy(new JobTracker(conf, clock));
  }
  
  public void testLostTaskTrackerCalledAfterExpiryTime() throws IOException {
    
    String tracker1 = "tracker_tracker1:1000";
    String tracker2 = "tracker_tracker2:1000";
    
    establishFirstContact(tracker1);
    
    // Wait long enough for tracker1 to be considered lost
    // We could have used a Mockito stub here, except we don't know how many 
    // times JobTracker calls getTime() on the clock, so a static mock
    // is appropriate.
    clock.advance(8 * 1000);
    
    establishFirstContact(tracker2);

    jobTracker.checkExpiredTrackers();
    
    // Now we check that JobTracker's lostTaskTracker() was called for tracker1
    // but not for tracker2.
    
    // We use an ArgumentCaptor to capture the task tracker object
    // in the lostTaskTracker() call, so we can perform an assertion on its
    // name. (We could also have used a custom matcher, see below.)
    // See http://mockito.googlecode.com/svn/branches/1.8.0/javadoc/org/mockito/Mockito.html#15
    ArgumentCaptor<TaskTracker> argument =
      ArgumentCaptor.forClass(TaskTracker.class);

    verify(jobTracker).lostTaskTracker(argument.capture());
    assertEquals(tracker1, argument.getValue().getTrackerName());
    
    // Check tracker2 was not lost by using the never() construct
    // We use a custom Hamcrest matcher to check that it was indeed tracker2
    // that didn't match (since tracker1 did match).
    // See http://mockito.googlecode.com/svn/branches/1.8.0/javadoc/org/mockito/Mockito.html#3
    verify(jobTracker, never()).lostTaskTracker(
	argThat(taskTrackerWithName(tracker2)));
  }
  
  private Matcher<TaskTracker> taskTrackerWithName(final String name) {
    return new ArgumentMatcher<TaskTracker>() {
      public boolean matches(Object taskTracker) {
          return name.equals(((TaskTracker) taskTracker).getTrackerName());
      }
    };
  }

  private void establishFirstContact(String tracker) throws IOException {
    TaskTrackerStatus status = new TaskTrackerStatus(tracker, 
        JobInProgress.convertTrackerNameToHostName(tracker));
    jobTracker.heartbeat(status, false, true, false, (short) 0);
  }
  
}