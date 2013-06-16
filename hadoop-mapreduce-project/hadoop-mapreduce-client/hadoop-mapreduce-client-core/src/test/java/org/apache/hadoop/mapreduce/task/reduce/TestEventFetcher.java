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
package org.apache.hadoop.mapreduce.task.reduce;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapTaskCompletionEventsUpdate;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Test;
import org.mockito.InOrder;

public class TestEventFetcher {

  @Test
  public void testConsecutiveFetch()
      throws IOException, InterruptedException {
    final int MAX_EVENTS_TO_FETCH = 100;
    TaskAttemptID tid = new TaskAttemptID("12345", 1, TaskType.REDUCE, 1, 1);

    TaskUmbilicalProtocol umbilical = mock(TaskUmbilicalProtocol.class);
    when(umbilical.getMapCompletionEvents(any(JobID.class),
        anyInt(), anyInt(), any(TaskAttemptID.class)))
      .thenReturn(getMockedCompletionEventsUpdate(0, 0));
    when(umbilical.getMapCompletionEvents(any(JobID.class),
        eq(0), eq(MAX_EVENTS_TO_FETCH), eq(tid)))
      .thenReturn(getMockedCompletionEventsUpdate(0, MAX_EVENTS_TO_FETCH));
    when(umbilical.getMapCompletionEvents(any(JobID.class),
        eq(MAX_EVENTS_TO_FETCH), eq(MAX_EVENTS_TO_FETCH), eq(tid)))
      .thenReturn(getMockedCompletionEventsUpdate(MAX_EVENTS_TO_FETCH,
          MAX_EVENTS_TO_FETCH));
    when(umbilical.getMapCompletionEvents(any(JobID.class),
        eq(MAX_EVENTS_TO_FETCH*2), eq(MAX_EVENTS_TO_FETCH), eq(tid)))
      .thenReturn(getMockedCompletionEventsUpdate(MAX_EVENTS_TO_FETCH*2, 3));

    @SuppressWarnings("unchecked")
    ShuffleScheduler<String,String> scheduler =
      mock(ShuffleScheduler.class);
    ExceptionReporter reporter = mock(ExceptionReporter.class);

    EventFetcherForTest<String,String> ef =
        new EventFetcherForTest<String,String>(tid, umbilical, scheduler,
            reporter, MAX_EVENTS_TO_FETCH);
    ef.getMapCompletionEvents();

    verify(reporter, never()).reportException(any(Throwable.class));
    InOrder inOrder = inOrder(umbilical);
    inOrder.verify(umbilical).getMapCompletionEvents(any(JobID.class),
        eq(0), eq(MAX_EVENTS_TO_FETCH), eq(tid));
    inOrder.verify(umbilical).getMapCompletionEvents(any(JobID.class),
        eq(MAX_EVENTS_TO_FETCH), eq(MAX_EVENTS_TO_FETCH), eq(tid));
    inOrder.verify(umbilical).getMapCompletionEvents(any(JobID.class),
        eq(MAX_EVENTS_TO_FETCH*2), eq(MAX_EVENTS_TO_FETCH), eq(tid));
    verify(scheduler, times(MAX_EVENTS_TO_FETCH*2 + 3)).resolve(
        any(TaskCompletionEvent.class));
  }

  private MapTaskCompletionEventsUpdate getMockedCompletionEventsUpdate(
      int startIdx, int numEvents) {
    ArrayList<TaskCompletionEvent> tceList =
        new ArrayList<TaskCompletionEvent>(numEvents);
    for (int i = 0; i < numEvents; ++i) {
      int eventIdx = startIdx + i;
      TaskCompletionEvent tce = new TaskCompletionEvent(eventIdx,
          new TaskAttemptID("12345", 1, TaskType.MAP, eventIdx, 0),
          eventIdx, true, TaskCompletionEvent.Status.SUCCEEDED,
          "http://somehost:8888");
      tceList.add(tce);
    }
    TaskCompletionEvent[] events = {};
    return new MapTaskCompletionEventsUpdate(tceList.toArray(events), false);
  }

  private static class EventFetcherForTest<K,V> extends EventFetcher<K,V> {

    public EventFetcherForTest(TaskAttemptID reduce,
        TaskUmbilicalProtocol umbilical, ShuffleScheduler<K,V> scheduler,
        ExceptionReporter reporter, int maxEventsToFetch) {
      super(reduce, umbilical, scheduler, reporter, maxEventsToFetch);
    }

    @Override
    public int getMapCompletionEvents()
        throws IOException, InterruptedException {
      return super.getMapCompletionEvents();
    }

  }
}
