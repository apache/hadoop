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
package org.apache.hadoop.yarn.sls.scheduler;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Queue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.exceptions.YarnException;

@Private
@Unstable
public class TaskRunner {
  @Private
  @Unstable
  public abstract static class Task implements Runnable, Delayed {
    private long start;
    private long end;
    private long nextRun;
    private long startTime;
    private long endTime;
    private long repeatInterval;
    private Queue<Task> queue;

    public Task(){}
    
    //values in milliseconds, start/end are milliseconds from now
    public void init(long startTime, long endTime, long repeatInterval) {
      if (endTime - startTime < 0) {
        throw new IllegalArgumentException(MessageFormat.format(
          "endTime[{0}] cannot be smaller than startTime[{1}]", endTime, 
          startTime));
      }
      if (repeatInterval < 0) {
        throw new IllegalArgumentException(MessageFormat.format(
          "repeatInterval[{0}] cannot be less than 1", repeatInterval));
      }
      if ((endTime - startTime) % repeatInterval != 0) {
        throw new IllegalArgumentException(MessageFormat.format(
          "Invalid parameters: (endTime[{0}] - startTime[{1}]) " +
                  "% repeatInterval[{2}] != 0",
          endTime, startTime, repeatInterval));        
      }
      start = startTime;
      end = endTime;
      this.repeatInterval = repeatInterval;
    }

    private void timeRebase(long now) {
      startTime = now + start;
      endTime = now + end;
      this.nextRun = startTime;
    }
    
    //values in milliseconds, start is milliseconds from now
    //it only executes firstStep()
    public void init(long startTime) {
      init(startTime, startTime, 1);
    }

    private void setQueue(Queue<Task> queue) {
      this.queue = queue;
    }

    @Override
    public final void run() {
      try {
        if (nextRun == startTime) {
          firstStep();
          nextRun += repeatInterval;
          if (nextRun <= endTime) {
            queue.add(this);          
          }
        } else if (nextRun < endTime) {
          middleStep();
          nextRun += repeatInterval;
          queue.add(this);
        } else {
          lastStep();
        }
      } catch (Exception e) {
        e.printStackTrace();
        Thread.getDefaultUncaughtExceptionHandler()
            .uncaughtException(Thread.currentThread(), e);
      }
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(nextRun - System.currentTimeMillis(),
        TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      if (!(o instanceof Task)) {
        throw new IllegalArgumentException("Parameter must be a Task instance");
      }
      Task other = (Task) o;
      return (int) Math.signum(nextRun - other.nextRun);
    }


    public abstract void firstStep() throws Exception;

    public abstract void middleStep() throws Exception;

    public abstract void lastStep() throws Exception;

    public void setEndTime(long et) {
      endTime = et;
    }
  }

  private DelayQueue queue;
  private int threadPoolSize;
  private ThreadPoolExecutor executor;
  private long startTimeMS = 0;
  
  public TaskRunner() {
    queue = new DelayQueue();
  }

  public void setQueueSize(int threadPoolSize) {
    this.threadPoolSize = threadPoolSize;
  }

  @SuppressWarnings("unchecked")
  public void start() {
    if (executor != null) {
      throw new IllegalStateException("Already started");
    }
    DelayQueue preStartQueue = queue;

    queue = new DelayQueue();
    executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0,
      TimeUnit.MILLISECONDS, queue);
    executor.prestartAllCoreThreads();

    startTimeMS = System.currentTimeMillis();
    for (Object d : preStartQueue) {
      schedule((Task) d, startTimeMS);
    }
  }
  
  public void stop() {
    executor.shutdownNow();
  }

  @SuppressWarnings("unchecked")
  private void schedule(Task task, long timeNow) {
    task.timeRebase(timeNow);
    task.setQueue(queue);
    queue.add(task);
  }

  public void schedule(Task task) {
    schedule(task, System.currentTimeMillis());
  }

  public long getStartTimeMS() {
    return this.startTimeMS;
  }
}
