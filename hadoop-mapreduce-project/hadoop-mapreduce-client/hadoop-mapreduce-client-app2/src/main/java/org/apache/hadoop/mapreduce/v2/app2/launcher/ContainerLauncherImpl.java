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

package org.apache.hadoop.mapreduce.v2.app2.launcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventLaunchFailed;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventLaunched;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventStopFailed;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventType;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


// TODO XXX: See what part of this lifecycle and state management can be simplified.
// Ideally, no state - only sendStart / sendStop.

// TODO XXX: Review this entire code and clean it up.

/**
 * This class is responsible for launching of containers.
 */
public class ContainerLauncherImpl extends AbstractService implements
    ContainerLauncher {

  // TODO XXX Ensure the same thread is used to launch / stop the same container. Or - ensure event ordering.
  static final Log LOG = LogFactory.getLog(ContainerLauncherImpl.class);

  private ConcurrentHashMap<ContainerId, Container> containers = 
    new ConcurrentHashMap<ContainerId, Container>(); 
  private AppContext context;
  protected ThreadPoolExecutor launcherPool;
  protected static final int INITIAL_POOL_SIZE = 10;
  private int limitOnPoolSize;
  private Thread eventHandlingThread;
  protected BlockingQueue<NMCommunicatorEvent> eventQueue =
      new LinkedBlockingQueue<NMCommunicatorEvent>();
  YarnRPC rpc;

  private Container getContainer(NMCommunicatorEvent event) {
    ContainerId id = event.getContainerId();
    Container c = containers.get(id);
    if(c == null) {
      c = new Container(event.getContainerId(),
          event.getNodeId().toString(), event.getContainerToken());
      Container old = containers.putIfAbsent(id, c);
      if(old != null) {
        c = old;
      }
    }
    return c;
  }
  
  private void removeContainerIfDone(ContainerId id) {
    Container c = containers.get(id);
    if(c != null && c.isCompletelyDone()) {
      containers.remove(id);
    }
  }
  
  private static enum ContainerState {
    PREP, FAILED, RUNNING, DONE, KILLED_BEFORE_LAUNCH
  }

  private class Container {
    private ContainerState state;
    // store enough information to be able to cleanup the container
    private ContainerId containerID;
    final private String containerMgrAddress;
    private ContainerToken containerToken;
    
    public Container(ContainerId containerID,
        String containerMgrAddress, ContainerToken containerToken) {
      this.state = ContainerState.PREP;
      this.containerMgrAddress = containerMgrAddress;
      this.containerID = containerID;
      this.containerToken = containerToken;
    }
    
    public synchronized boolean isCompletelyDone() {
      return state == ContainerState.DONE || state == ContainerState.FAILED;
    }
    
    @SuppressWarnings("unchecked")
    public synchronized void launch(NMCommunicatorLaunchRequestEvent event) {
      LOG.info("Launching Container with Id: " + event.getContainerId());
      if(this.state == ContainerState.KILLED_BEFORE_LAUNCH) {
        state = ContainerState.DONE;
        sendContainerLaunchFailedMsg(event.getContainerId(), 
            "Container was killed before it was launched");
        return;
      }
      
      ContainerManager proxy = null;
      try {

        proxy = getCMProxy(containerID, containerMgrAddress,
            containerToken);

        // Construct the actual Container
        ContainerLaunchContext containerLaunchContext =
          event.getContainerLaunchContext();

        // Now launch the actual container
        StartContainerRequest startRequest = Records
          .newRecord(StartContainerRequest.class);
        startRequest.setContainerLaunchContext(containerLaunchContext);
        StartContainerResponse response = proxy.startContainer(startRequest);

        ByteBuffer portInfo = response
          .getServiceResponse(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID);
        int port = -1;
        if(portInfo != null) {
          port = ShuffleHandler.deserializeMetaData(portInfo);
        }
        LOG.info("Shuffle port returned by ContainerManager for "
            + containerID + " : " + port);

        if(port < 0) {
          this.state = ContainerState.FAILED;
          throw new IllegalStateException("Invalid shuffle port number "
              + port + " returned for " + containerID);
        }

        // after launching, send launched event to task attempt to move
        // it from ASSIGNED to RUNNING state
        context.getEventHandler().handle(new AMContainerEventLaunched(containerID, port));
        this.state = ContainerState.RUNNING;
      } catch (Throwable t) {
        String message = "Container launch failed for " + containerID + " : "
            + StringUtils.stringifyException(t);
        this.state = ContainerState.FAILED;
        sendContainerLaunchFailedMsg(containerID, message);
      } finally {
        if (proxy != null) {
          ContainerLauncherImpl.this.rpc.stopProxy(proxy, getConfig());
        }
      }
    }
    
    @SuppressWarnings("unchecked")
    public synchronized void kill() {

      if(isCompletelyDone()) { 
        return;
      }
      if(this.state == ContainerState.PREP) {
        this.state = ContainerState.KILLED_BEFORE_LAUNCH;
      } else {
        LOG.info("KILLING ContainerId: " + containerID);

        ContainerManager proxy = null;
        try {
          proxy = getCMProxy(this.containerID, this.containerMgrAddress,
              this.containerToken);

            // kill the remote container if already launched
            StopContainerRequest stopRequest = Records
              .newRecord(StopContainerRequest.class);
            stopRequest.setContainerId(this.containerID);
            proxy.stopContainer(stopRequest);
            // If stopContainer returns without an error, assuming the stop made
            // it over to the NodeManager.
          context.getEventHandler().handle(
              new AMContainerEvent(containerID, AMContainerEventType.C_NM_STOP_SENT));
        } catch (Throwable t) {

          // ignore the cleanup failure
          String message = "cleanup failed for container "
            + this.containerID + " : "
            + StringUtils.stringifyException(t);
          context.getEventHandler().handle(
              new AMContainerEventStopFailed(containerID, message));
          LOG.warn(message);
          this.state = ContainerState.DONE;
          return;
        } finally {
          if (proxy != null) {
            ContainerLauncherImpl.this.rpc.stopProxy(proxy, getConfig());
          }
        }
        this.state = ContainerState.DONE;
      }
    }
  }

  public ContainerLauncherImpl(AppContext context) {
    super(ContainerLauncherImpl.class.getName());
    this.context = context;
  }

  @Override
  public synchronized void init(Configuration config) {
    Configuration conf = new Configuration(config);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    this.limitOnPoolSize = conf.getInt(
        MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT);
    LOG.info("Upper limit on the thread pool size is " + this.limitOnPoolSize);
    this.rpc = createYarnRPC(conf);
    super.init(conf);
  }
  
  protected YarnRPC createYarnRPC(Configuration conf) {
    return YarnRPC.create(conf);
  }

  public void start() {

    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
        "ContainerLauncher #%d").setDaemon(true).build();

    // Start with a default core-pool size of 10 and change it dynamically.
    launcherPool = new ThreadPoolExecutor(INITIAL_POOL_SIZE,
        Integer.MAX_VALUE, 1, TimeUnit.HOURS,
        new LinkedBlockingQueue<Runnable>(),
        tf);
    eventHandlingThread = new Thread() {
      @Override
      public void run() {
        NMCommunicatorEvent event = null;
        while (!Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }
          int poolSize = launcherPool.getCorePoolSize();

          // See if we need up the pool size only if haven't reached the
          // maximum limit yet.
          if (poolSize != limitOnPoolSize) {

            // nodes where containers will run at *this* point of time. This is
            // *not* the cluster size and doesn't need to be.
            int numNodes = context.getAllNodes().size();
            int idealPoolSize = Math.min(limitOnPoolSize, numNodes);

            if (poolSize < idealPoolSize) {
              // Bump up the pool size to idealPoolSize+INITIAL_POOL_SIZE, the
              // later is just a buffer so we are not always increasing the
              // pool-size
              int newPoolSize = Math.min(limitOnPoolSize, idealPoolSize
                  + INITIAL_POOL_SIZE);
              LOG.info("Setting ContainerLauncher pool size to " + newPoolSize
                  + " as number-of-nodes to talk to is " + numNodes);
              launcherPool.setCorePoolSize(newPoolSize);
            }
          }

          // the events from the queue are handled in parallel
          // using a thread pool
          launcherPool.execute(createEventProcessor(event));

          // TODO: Group launching of multiple containers to a single
          // NodeManager into a single connection
        }
      }
    };
    eventHandlingThread.setName("ContainerLauncher Event Handler");
    eventHandlingThread.start();
    super.start();
  }

  private void shutdownAllContainers() {
    for (Container ct : this.containers.values()) {
      if (ct != null) {
        ct.kill();
      }
    }
  }

  public void stop() {
    // shutdown any containers that might be left running
    shutdownAllContainers();
    eventHandlingThread.interrupt();
    launcherPool.shutdownNow();
    super.stop();
  }

  protected EventProcessor createEventProcessor(NMCommunicatorEvent event) {
    return new EventProcessor(event);
  }

  protected ContainerManager getCMProxy(ContainerId containerID,
      final String containerManagerBindAddr, ContainerToken containerToken)
      throws IOException {

    final InetSocketAddress cmAddr =
        NetUtils.createSocketAddr(containerManagerBindAddr);
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    if (UserGroupInformation.isSecurityEnabled()) {
      Token<ContainerTokenIdentifier> token =
          ProtoUtils.convertFromProtoFormat(containerToken, cmAddr);
      // the user in createRemoteUser in this context has to be ContainerID
      user = UserGroupInformation.createRemoteUser(containerID.toString());
      user.addToken(token);
    }

    ContainerManager proxy = user
        .doAs(new PrivilegedAction<ContainerManager>() {
          @Override
          public ContainerManager run() {
            return (ContainerManager) rpc.getProxy(ContainerManager.class,
                cmAddr, getConfig());
          }
        });
    return proxy;
  }


  /**
   * Setup and start the container on remote nodemanager.
   */
  class EventProcessor implements Runnable {
    private NMCommunicatorEvent event;

    EventProcessor(NMCommunicatorEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());

      // Load ContainerManager tokens before creating a connection.
      // TODO: Do it only once per NodeManager.
      ContainerId containerID = event.getContainerId();

      Container c = getContainer(event);
      switch(event.getType()) {

      case CONTAINER_LAUNCH_REQUEST:
        NMCommunicatorLaunchRequestEvent launchEvent
            = (NMCommunicatorLaunchRequestEvent) event;
        c.launch(launchEvent);
        break;

      case CONTAINER_STOP_REQUEST:
        c.kill();
        break;
      }
      removeContainerIfDone(containerID);
    }
  }

  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(ContainerId containerId,
      String message) {
    LOG.error(message);
    context.getEventHandler().handle(new AMContainerEventLaunchFailed(containerId, message));
  }

  @Override
  public void handle(NMCommunicatorEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }
}
