package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;

public interface GetTaskAttemptCompletionEventsResponse {
  public abstract List<TaskAttemptCompletionEvent> getCompletionEventList();
  public abstract TaskAttemptCompletionEvent getCompletionEvent(int index);
  public abstract int getCompletionEventCount();
  
  public abstract void addAllCompletionEvents(List<TaskAttemptCompletionEvent> eventList);
  public abstract void addCompletionEvent(TaskAttemptCompletionEvent event);
  public abstract void removeCompletionEvent(int index);
  public abstract void clearCompletionEvents();  
}
