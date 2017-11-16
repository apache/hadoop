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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.NoopAMPreemptionPolicy;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;
import org.slf4j.event.Level;

public class MRAppBenchmark {

  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  /**
   * Runs memory and time benchmark with Mock MRApp.
   * @param app Application to submit
   * @throws Exception On application failure
   */
  public void run(MRApp app) throws Exception {
    GenericTestUtils.setRootLogLevel(Level.WARN);
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
        ClientService clientService, AppContext context) {
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
          throw new YarnRuntimeException(e);
        }
      }
      @Override
      protected void serviceStart() throws Exception {
        thread = new Thread(new Runnable() {
          @Override
          @SuppressWarnings("unchecked")
          public void run() {
            ContainerAllocatorEvent event = null;
            while (!Thread.currentThread().isInterrupted()) {
              try {
                if (concurrentRunningTasks < maxConcurrentRunningTasks) {
                  event = eventQueue.take();
                  ContainerId cId =
                      ContainerId.newContainerId(getContext()
                        .getApplicationAttemptId(), containerCount++);

                  //System.out.println("Allocating " + containerCount);
                  
                  Container container = 
                      recordFactory.newRecordInstance(Container.class);
                  container.setId(cId);
                  NodeId nodeId = NodeId.newInstance("dummy", 1234);
                  container.setNodeId(nodeId);
                  container.setContainerToken(null);
                  container.setNodeHttpAddress("localhost:8042");
                  getContext().getEventHandler()
                      .handle(
                      new TaskAttemptContainerAssignedEvent(event
                          .getAttemptID(), container, null));
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
        super.serviceStart();
      }

      @Override
      protected void serviceStop() throws Exception {
        if (thread != null) {
          thread.interrupt();
        }
        super.serviceStop();
      }
    }
  }

  @Test
  public void benchmark1() throws Exception {
    int maps = 100; // Adjust for benchmarking. Start with thousands.
    int reduces = 0;
    System.out.println("Running benchmark with maps:"+maps +
        " reduces:"+reduces);
    run(new MRApp(maps, reduces, true, this.getClass().getName(), true) {

      @Override
      protected ContainerAllocator createContainerAllocator(
          ClientService clientService, AppContext context) {

        AMPreemptionPolicy policy = new NoopAMPreemptionPolicy();
        return new RMContainerAllocator(clientService, context, policy) {
          @Override
          protected ApplicationMasterProtocol createSchedulerProxy() {
            return new ApplicationMasterProtocol() {

              @Override
              public RegisterApplicationMasterResponse
                  registerApplicationMaster(
                      RegisterApplicationMasterRequest request)
                      throws IOException {
                RegisterApplicationMasterResponse response =
                    Records.newRecord(RegisterApplicationMasterResponse.class);
                response.setMaximumResourceCapability(Resource.newInstance(
                  10240, 1));
                return response;
              }

              @Override
              public FinishApplicationMasterResponse finishApplicationMaster(
                  FinishApplicationMasterRequest request)
                  throws IOException {
                FinishApplicationMasterResponse response =
                    Records.newRecord(FinishApplicationMasterResponse.class);
                return response;
              }

              @Override
              public AllocateResponse allocate(AllocateRequest request)
                  throws IOException {

                AllocateResponse response =
                    Records.newRecord(AllocateResponse.class);
                List<ResourceRequest> askList = request.getAskList();
                List<Container> containers = new ArrayList<Container>();
                for (ResourceRequest req : askList) {
                  if (!ResourceRequest.isAnyLocation(req.getResourceName())) {
                    continue;
                  }
                  int numContainers = req.getNumContainers();
                  for (int i = 0; i < numContainers; i++) {
                    ContainerId containerId =
                        ContainerId.newContainerId(
                          getContext().getApplicationAttemptId(),
                          request.getResponseId() + i);
                    containers.add(Container.newInstance(containerId,
                        NodeId.newInstance(
                            "host" + containerId.getContainerId(), 2345),
                        "host" + containerId.getContainerId() + ":5678",
                        req.getCapability(), req.getPriority(), null));
                  }
                }

                response.setAllocatedContainers(containers);
                response.setResponseId(request.getResponseId() + 1);
                response.setNumClusterNodes(350);
                return response;
              }
            };
          }
        };
      }
    });
  }

  @Test
  public void benchmark2() throws Exception {
    int maps = 100; // Adjust for benchmarking, start with a couple of thousands
    int reduces = 50;
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
