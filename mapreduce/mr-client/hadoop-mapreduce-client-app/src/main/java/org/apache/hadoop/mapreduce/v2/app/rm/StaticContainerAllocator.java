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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.MRConstants;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * Reads the static list of NodeManager from config file and allocate 
 * containers.
 */
public class StaticContainerAllocator extends AbstractService 
    implements ContainerAllocator {

  private static final Log LOG = 
    LogFactory.getLog(StaticContainerAllocator.class);

  private AppContext context;
  private volatile boolean stopped;
  private BlockingQueue<ContainerAllocatorEvent> eventQueue =
      new LinkedBlockingQueue<ContainerAllocatorEvent>();
  private Thread allocatorThread;

  private int containerCount;

  private List<String> containerMgrList;
  private int nextIndex;

  public StaticContainerAllocator(AppContext context) {
    super("StaticContainerAllocator");
    this.context = context;
  }

  protected List<String> getContainerMgrList(Configuration conf)
      throws IOException {
    Path jobSubmitDir = FileContext.getLocalFSFileContext().makeQualified(
        new Path(new File(MRConstants.JOB_SUBMIT_DIR).getAbsolutePath()));
    Path jobConfFile = new Path(jobSubmitDir, MRConstants.JOB_CONF_FILE);
    conf.addResource(jobConfFile);
    String[] containerMgrHosts = 
      conf.getStrings(MRConstants.NM_HOSTS_CONF_KEY);
    return Arrays.asList(containerMgrHosts);
  }

  @Override
  public void init(Configuration conf) {
    try {
      containerMgrList = getContainerMgrList(conf);
    } catch (IOException e) {
      throw new YarnException("Cannot get container-managers list ", e);
    }

    if (containerMgrList.size() == 0) {
      throw new YarnException("No of Container Managers are zero.");
    }
    super.init(conf);
  }

  @Override
  public void start() {
    allocatorThread = new Thread(new Allocator());
    allocatorThread.start();
    super.start();
  }

  @Override
  public void stop() {
    stopped = true;
    allocatorThread.interrupt();
    try {
      allocatorThread.join();
    } catch (InterruptedException ie) {
      LOG.debug("Interruped Exception while stopping", ie);
    }
    super.stop();
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  private class Allocator implements Runnable {
    @Override
    public void run() {
      ContainerAllocatorEvent event = null;
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          event = eventQueue.take();
          LOG.info("Processing the event " + event.toString());
          allocate(event);
        } catch (InterruptedException e) {
          return;
        }
      }
    }

    private void allocate(ContainerAllocatorEvent event) {
      // allocate the container in round robin fashion on
      // container managers
      if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
        if (nextIndex < containerMgrList.size()) {
          String containerMgr = containerMgrList.get(nextIndex);
          ContainerId containerID = generateContainerID();
          
          Container container = RecordFactoryProvider.getRecordFactory(null)
              .newRecordInstance(Container.class);
          container.setId(containerID);
          container.setContainerManagerAddress(containerMgr);
          container.setContainerToken(null);
          container.setNodeHttpAddress("localhost:9999");
          context.getEventHandler().handle(
            new TaskAttemptContainerAssignedEvent(
                event.getAttemptID(), container));
        }
      }
    }

    private ContainerId generateContainerID() {
      ContainerId cId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ContainerId.class);
      cId.setAppId(context.getApplicationID());
      cId.setId(containerCount++);
      return cId;
    }
  }
}
