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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.junit.Test;

public class TestEvents {

  /**
   * test a getters of TaskAttemptFinishedEvent and TaskAttemptFinished
   * 
   * @throws Exception
   */
  @Test(timeout = 10000)
  public void testTaskAttemptFinishedEvent() throws Exception {

    JobID jid = new JobID("001", 1);
    TaskID tid = new TaskID(jid, TaskType.REDUCE, 2);
    TaskAttemptID taskAttemptId = new TaskAttemptID(tid, 3);
    Counters counters = new Counters();
    TaskAttemptFinishedEvent test = new TaskAttemptFinishedEvent(taskAttemptId,
        TaskType.REDUCE, "TEST", 123L, "RAKNAME", "HOSTNAME", "STATUS",
        counters);
    assertEquals(test.getAttemptId().toString(), taskAttemptId.toString());

    assertEquals(test.getCounters(), counters);
    assertEquals(test.getFinishTime(), 123L);
    assertEquals(test.getHostname(), "HOSTNAME");
    assertEquals(test.getRackName(), "RAKNAME");
    assertEquals(test.getState(), "STATUS");
    assertEquals(test.getTaskId(), tid);
    assertEquals(test.getTaskStatus(), "TEST");
    assertEquals(test.getTaskType(), TaskType.REDUCE);

  }

  /**
   * simple test JobPriorityChangeEvent and JobPriorityChange
   * 
   * @throws Exception
   */

  @Test(timeout = 10000)
  public void testJobPriorityChange() throws Exception {
    org.apache.hadoop.mapreduce.JobID jid = new JobID("001", 1);
    JobPriorityChangeEvent test = new JobPriorityChangeEvent(jid,
        JobPriority.LOW);
    assertEquals(test.getJobId().toString(), jid.toString());
    assertEquals(test.getPriority(), JobPriority.LOW);

  }
  
  @Test(timeout = 10000)
  public void testJobQueueChange() throws Exception {
    org.apache.hadoop.mapreduce.JobID jid = new JobID("001", 1);
    JobQueueChangeEvent test = new JobQueueChangeEvent(jid,
        "newqueue");
    assertEquals(test.getJobId().toString(), jid.toString());
    assertEquals(test.getJobQueueName(), "newqueue");
  }

  /**
   * simple test TaskUpdatedEvent and TaskUpdated
   * 
   * @throws Exception
   */
  @Test(timeout = 10000)
  public void testTaskUpdated() throws Exception {
    JobID jid = new JobID("001", 1);
    TaskID tid = new TaskID(jid, TaskType.REDUCE, 2);
    TaskUpdatedEvent test = new TaskUpdatedEvent(tid, 1234L);
    assertEquals(test.getTaskId().toString(), tid.toString());
    assertEquals(test.getFinishTime(), 1234L);

  }

  /*
   * test EventReader EventReader should read the list of events and return
   * instance of HistoryEvent Different HistoryEvent should have a different
   * datum.
   */
  @Test(timeout = 10000)
  public void testEvents() throws Exception {

    EventReader reader = new EventReader(new DataInputStream(
        new ByteArrayInputStream(getEvents())));
    HistoryEvent e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.JOB_PRIORITY_CHANGED));
    assertEquals("ID", ((JobPriorityChange) e.getDatum()).jobid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.JOB_STATUS_CHANGED));
    assertEquals("ID", ((JobStatusChanged) e.getDatum()).jobid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.TASK_UPDATED));
    assertEquals("ID", ((TaskUpdated) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals("task_1_2_r03_4",
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.JOB_KILLED));
    assertEquals("ID",
        ((JobUnsuccessfulCompletion) e.getDatum()).jobid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_STARTED));
    assertEquals("task_1_2_r03_4",
        ((TaskAttemptStarted) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_FINISHED));
    assertEquals("task_1_2_r03_4",
        ((TaskAttemptFinished) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals("task_1_2_r03_4",
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals("task_1_2_r03_4",
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_STARTED));
    assertEquals("task_1_2_r03_4",
        ((TaskAttemptStarted) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_FINISHED));
    assertEquals("task_1_2_r03_4",
        ((TaskAttemptFinished) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals("task_1_2_r03_4",
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals("task_1_2_r03_4",
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    reader.close();
  }

  /*
   * makes array of bytes with History events
   */
  private byte[] getEvents() throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    FSDataOutputStream fsOutput = new FSDataOutputStream(output,
        new FileSystem.Statistics("scheme"));
    EventWriter writer = new EventWriter(fsOutput);
    writer.write(getJobPriorityChangedEvent());
    writer.write(getJobStatusChangedEvent());
    writer.write(getTaskUpdatedEvent());
    writer.write(getReduceAttemptKilledEvent());
    writer.write(getJobKilledEvent());
    writer.write(getSetupAttemptStartedEvent());
    writer.write(getTaskAttemptFinishedEvent());
    writer.write(getSetupAttemptFieledEvent());
    writer.write(getSetupAttemptKilledEvent());
    writer.write(getCleanupAttemptStartedEvent());
    writer.write(getCleanupAttemptFinishedEvent());
    writer.write(getCleanupAttemptFiledEvent());
    writer.write(getCleanupAttemptKilledEvent());

    writer.flush();
    writer.close();

    return output.toByteArray();
  }

  private FakeEvent getCleanupAttemptKilledEvent() {
    FakeEvent result = new FakeEvent(EventType.CLEANUP_ATTEMPT_KILLED);

    result.setDatum(getTaskAttemptUnsuccessfulCompletion());
    return result;
  }

  private FakeEvent getCleanupAttemptFiledEvent() {
    FakeEvent result = new FakeEvent(EventType.CLEANUP_ATTEMPT_FAILED);

    result.setDatum(getTaskAttemptUnsuccessfulCompletion());
    return result;
  }

  private TaskAttemptUnsuccessfulCompletion getTaskAttemptUnsuccessfulCompletion() {
    TaskAttemptUnsuccessfulCompletion datum = new TaskAttemptUnsuccessfulCompletion();
    datum.attemptId = "attempt_1_2_r3_4_5";
    datum.clockSplits = Arrays.asList(1, 2, 3);
    datum.cpuUsages = Arrays.asList(100, 200, 300);
    datum.error = "Error";
    datum.finishTime = 2;
    datum.hostname = "hostname";
    datum.rackname = "rackname";
    datum.physMemKbytes = Arrays.asList(1000, 2000, 3000);
    datum.taskid = "task_1_2_r03_4";
    datum.port = 1000;
    datum.taskType = "REDUCE";
    datum.status = "STATUS";
    datum.counters = getCounters();
    datum.vMemKbytes = Arrays.asList(1000, 2000, 3000);
    return datum;
  }

  private JhCounters getCounters() {
    JhCounters counters = new JhCounters();
    counters.groups = new ArrayList<JhCounterGroup>(0);
    counters.name = "name";
    return counters;
  }

  private FakeEvent getCleanupAttemptFinishedEvent() {
    FakeEvent result = new FakeEvent(EventType.CLEANUP_ATTEMPT_FINISHED);
    TaskAttemptFinished datum = new TaskAttemptFinished();
    datum.attemptId = "attempt_1_2_r3_4_5";

    datum.counters = getCounters();
    datum.finishTime = 2;
    datum.hostname = "hostname";
    datum.rackname = "rackName";
    datum.state = "state";
    datum.taskid = "task_1_2_r03_4";
    datum.taskStatus = "taskStatus";
    datum.taskType = "REDUCE";
    result.setDatum(datum);
    return result;
  }

  private FakeEvent getCleanupAttemptStartedEvent() {
    FakeEvent result = new FakeEvent(EventType.CLEANUP_ATTEMPT_STARTED);
    TaskAttemptStarted datum = new TaskAttemptStarted();

    datum.attemptId = "attempt_1_2_r3_4_5";
    datum.avataar = "avatar";
    datum.containerId = "containerId";
    datum.httpPort = 10000;
    datum.locality = "locality";
    datum.shufflePort = 10001;
    datum.startTime = 1;
    datum.taskid = "task_1_2_r03_4";
    datum.taskType = "taskType";
    datum.trackerName = "trackerName";
    result.setDatum(datum);
    return result;
  }

  private FakeEvent getSetupAttemptKilledEvent() {
    FakeEvent result = new FakeEvent(EventType.SETUP_ATTEMPT_KILLED);
    result.setDatum(getTaskAttemptUnsuccessfulCompletion());
    return result;
  }

  private FakeEvent getSetupAttemptFieledEvent() {
    FakeEvent result = new FakeEvent(EventType.SETUP_ATTEMPT_FAILED);

    result.setDatum(getTaskAttemptUnsuccessfulCompletion());
    return result;
  }

  private FakeEvent getTaskAttemptFinishedEvent() {
    FakeEvent result = new FakeEvent(EventType.SETUP_ATTEMPT_FINISHED);
    TaskAttemptFinished datum = new TaskAttemptFinished();

    datum.attemptId = "attempt_1_2_r3_4_5";
    datum.counters = getCounters();
    datum.finishTime = 2;
    datum.hostname = "hostname";
    datum.rackname = "rackname";
    datum.state = "state";
    datum.taskid = "task_1_2_r03_4";
    datum.taskStatus = "taskStatus";
    datum.taskType = "REDUCE";
    result.setDatum(datum);
    return result;
  }

  private FakeEvent getSetupAttemptStartedEvent() {
    FakeEvent result = new FakeEvent(EventType.SETUP_ATTEMPT_STARTED);
    TaskAttemptStarted datum = new TaskAttemptStarted();
    datum.attemptId = "ID";
    datum.avataar = "avataar";
    datum.containerId = "containerId";
    datum.httpPort = 10000;
    datum.locality = "locality";
    datum.shufflePort = 10001;
    datum.startTime = 1;
    datum.taskid = "task_1_2_r03_4";
    datum.taskType = "taskType";
    datum.trackerName = "trackerName";
    result.setDatum(datum);
    return result;
  }

  private FakeEvent getJobKilledEvent() {
    FakeEvent result = new FakeEvent(EventType.JOB_KILLED);
    JobUnsuccessfulCompletion datum = new JobUnsuccessfulCompletion();
    datum.setFinishedMaps(1);
    datum.setFinishedReduces(2);
    datum.setFinishTime(3L);
    datum.setJobid("ID");
    datum.setJobStatus("STATUS");
    datum.setDiagnostics(JobImpl.JOB_KILLED_DIAG);
    result.setDatum(datum);
    return result;
  }

  private FakeEvent getReduceAttemptKilledEvent() {
    FakeEvent result = new FakeEvent(EventType.REDUCE_ATTEMPT_KILLED);

    result.setDatum(getTaskAttemptUnsuccessfulCompletion());
    return result;
  }

  private FakeEvent getJobPriorityChangedEvent() {
    FakeEvent result = new FakeEvent(EventType.JOB_PRIORITY_CHANGED);
    JobPriorityChange datum = new JobPriorityChange();
    datum.jobid = "ID";
    datum.priority = "priority";
    result.setDatum(datum);
    return result;
  }

  private FakeEvent getJobStatusChangedEvent() {
    FakeEvent result = new FakeEvent(EventType.JOB_STATUS_CHANGED);
    JobStatusChanged datum = new JobStatusChanged();
    datum.jobid = "ID";
    datum.jobStatus = "newStatus";
    result.setDatum(datum);
    return result;
  }

  private FakeEvent getTaskUpdatedEvent() {
    FakeEvent result = new FakeEvent(EventType.TASK_UPDATED);
    TaskUpdated datum = new TaskUpdated();
    datum.finishTime = 2;
    datum.taskid = "ID";
    result.setDatum(datum);
    return result;
  }

  private class FakeEvent implements HistoryEvent {
    private EventType eventType;
    private Object datum;

    public FakeEvent(EventType eventType) {
      this.eventType = eventType;
    }

    @Override
    public EventType getEventType() {
      return eventType;
    }

    @Override
    public Object getDatum() {

      return datum;
    }

    @Override
    public void setDatum(Object datum) {
      this.datum = datum;
    }

  }

}
