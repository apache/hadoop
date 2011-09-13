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

package org.apache.hadoop.mapreduce.v2.app;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MRAppBenchmark {

  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  /**
   * Runs memory and time benchmark with Mock MRApp.
   */
  public void run(MRApp app) throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.WARN);
    long startTime = System.currentTimeMillis();
    Job job = app.submit(new Configuration());
    while (!job.getReport().getJobState().equals(JobState.SUCCEEDED)) {
      printStat(job, startTime);
      Thread.sleep(2000);
    }
    printStat(job, startTime);
  }

  private void printStat(Job job, long startTime) throws Exception {
    long currentTime = System.currentTimeMillis();
    Runtime.getRuntime().gc();
    long mem = Runtime.getRuntime().totalMemory() 
      - Runtime.getRuntime().freeMemory();
    System.out.println("JobState:" + job.getState() +
        " CompletedMaps:" + job.getCompletedMaps() +
        " CompletedReduces:" + job.getCompletedReduces() +
        " Memory(total-free)(KB):" + mem/1024 +
        " ElapsedTime(ms):" + (currentTime - startTime));
  }

  //Throttles the maximum number of concurrent running tasks.
  //This affects the memory requirement since 
  //org.apache.hadoop.mapred.MapTask/ReduceTask is loaded in memory for all
  //running task and discarded once the task is launched.
  static class ThrottledMRApp extends MRApp {

    int maxConcurrentRunningTasks;
    volatile int concurrentRunningTasks;
    ThrottledMRApp(int maps, int reduces, int maxConcurrentRunningTasks) {
      super(maps, reduces, true, "ThrottledMRApp", true);
      this.maxConcurrentRunningTasks = maxConcurrentRunningTasks;
    }
    
    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      super.attemptLaunched(attemptID);
      //the task is launched and sends done immediately
      concurrentRunningTasks--;
    }
    
    @Override
    protected ContainerAllocator createContainerAllocator(
        ClientService clientService, AppContext context, boolean isLocal) {
      return new ThrottledContainerAllocator();
    }
    
    class ThrottledContainerAllocator extends AbstractService 
        implements ContainerAllocator {
      private int containerCount;
      private Thread thread;
      private BlockingQueue<ContainerAllocatorEvent> eventQueue =
        new LinkedBlockingQueue<ContainerAllocatorEvent>();
      public ThrottledContainerAllocator() {
        super("ThrottledContainerAllocator");
      }
      @Override
      public void handle(ContainerAllocatorEvent event) {
        try {
          eventQueue.put(event);
        } catch (InterruptedException e) {
          throw new YarnException(e);
        }
      }
      @Override
      public void start() {
        thread = new Thread(new Runnable() {
          @Override
          public void run() {
            ContainerAllocatorEvent event = null;
            while (!Thread.currentThread().isInterrupted()) {
              try {
                if (concurrentRunningTasks < maxConcurrentRunningTasks) {
                  event = eventQueue.take();
                  ContainerId cId = 
                      recordFactory.newRecordInstance(ContainerId.class);
                  cId.setApplicationAttemptId(
                      getContext().getApplicationAttemptId());
                  cId.setId(containerCount++);
                  //System.out.println("Allocating " + containerCount);
                  
                  Container container = 
                      recordFactory.newRecordInstance(Container.class);
                  container.setId(cId);
                  NodeId nodeId = recordFactory.newRecordInstance(NodeId.class);
                  nodeId.setHost("dummy");
                  nodeId.setPort(1234);
                  container.setNodeId(nodeId);
                  container.setContainerToken(null);
                  container.setNodeHttpAddress("localhost:9999");
                  getContext().getEventHandler()
                      .handle(
                      new TaskAttemptContainerAssignedEvent(event
                          .getAttemptID(), container));
                  concurrentRunningTasks++;
                } else {
                  Thread.sleep(1000);
                }
              } catch (InterruptedException e) {
                System.out.println("Returning, interrupted");
                return;
              }
            }
          }
        });
        thread.start();
        super.start();
      }

      @Override
      public void stop() {
        thread.interrupt();
        super.stop();
      }
    }
  }

  public void benchmark1() throws Exception {
    int maps = 900;
    int reduces = 100;
    System.out.println("Running benchmark with maps:"+maps +
        " reduces:"+reduces);
    run(new MRApp(maps, reduces, true, this.getClass().getName(), true));
  }

  public void benchmark2() throws Exception {
    int maps = 4000;
    int reduces = 1000;
    int maxConcurrentRunningTasks = 500;
    
    System.out.println("Running benchmark with throttled running tasks with " +
        "maxConcurrentRunningTasks:" + maxConcurrentRunningTasks +
        " maps:" + maps + " reduces:" + reduces);
    run(new ThrottledMRApp(maps, reduces, maxConcurrentRunningTasks));
  }

  public static void main(String[] args) throws Exception {
    MRAppBenchmark benchmark = new MRAppBenchmark();
    benchmark.benchmark1();
    benchmark.benchmark2();
  }

}
