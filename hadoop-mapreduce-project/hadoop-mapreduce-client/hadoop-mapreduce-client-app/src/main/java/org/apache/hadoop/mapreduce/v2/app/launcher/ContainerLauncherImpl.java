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

package org.apache.hadoop.mapreduce.v2.app.launcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
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
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.service.AbstractService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class is responsible for launching of containers.
 */
public class ContainerLauncherImpl extends AbstractService implements
    ContainerLauncher {

  static final Log LOG = LogFactory.getLog(ContainerLauncherImpl.class);

  int nmTimeOut;

  private AppContext context;
  private ThreadPoolExecutor launcherPool;
  private static final int INITIAL_POOL_SIZE = 10;
  private int limitOnPoolSize;
  private Thread eventHandlingThread;
  private BlockingQueue<ContainerLauncherEvent> eventQueue =
      new LinkedBlockingQueue<ContainerLauncherEvent>();
  private RecordFactory recordFactory;
  //have a cache/map of UGIs so as to avoid creating too many RPC
  //client connection objects to the same NodeManager
  private ConcurrentMap<String, UserGroupInformation> ugiMap = 
    new ConcurrentHashMap<String, UserGroupInformation>();

  public ContainerLauncherImpl(AppContext context) {
    super(ContainerLauncherImpl.class.getName());
    this.context = context;
  }

  @Override
  public synchronized void init(Configuration conf) {
    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);
    this.limitOnPoolSize = conf.getInt(
        MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT);
    this.nmTimeOut = conf.getInt(ContainerLauncher.MR_AM_NM_COMMAND_TIMEOUT,
        ContainerLauncher.DEFAULT_NM__COMMAND_TIMEOUT);
    super.init(conf);
  }

  public void start() {

    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
        "ContainerLauncher #%d").setDaemon(true).build();

    // Start with a default core-pool size of 10 and change it dynamically.
    launcherPool = new ThreadPoolExecutor(INITIAL_POOL_SIZE,
        Integer.MAX_VALUE, 1, TimeUnit.HOURS,
        new LinkedBlockingQueue<Runnable>(),
        tf);
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        ContainerLauncherEvent event = null;
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
            int numNodes = ugiMap.size();
            int idealPoolSize = Math.min(limitOnPoolSize, numNodes);

            if (poolSize <= idealPoolSize) {
              // Bump up the pool size to idealPoolSize+INITIAL_POOL_SIZE, the
              // later is just a buffer so we are not always increasing the
              // pool-size
              int newPoolSize = idealPoolSize + INITIAL_POOL_SIZE;
              LOG.debug("Setting pool size to " + newPoolSize);
              launcherPool.setCorePoolSize(newPoolSize);
            }
          }

          // the events from the queue are handled in parallel
          // using a thread pool
          launcherPool.execute(new EventProcessor(event));

          // TODO: Group launching of multiple containers to a single
          // NodeManager into a single connection
        }
      }
    });
    eventHandlingThread.setName("ContainerLauncher Event Handler");
    eventHandlingThread.start();
    super.start();
  }

  public void stop() {
    eventHandlingThread.interrupt();
    launcherPool.shutdownNow();
    super.stop();
  }

  protected ContainerManager getCMProxy(
      final String containerManagerBindAddr, ContainerToken containerToken)
      throws IOException {

    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    if (UserGroupInformation.isSecurityEnabled()) {

      Token<ContainerTokenIdentifier> token = new Token<ContainerTokenIdentifier>(
          containerToken.getIdentifier().array(), containerToken
              .getPassword().array(), new Text(containerToken.getKind()),
          new Text(containerToken.getService()));
      // the user in createRemoteUser in this context is not important
      UserGroupInformation ugi = UserGroupInformation
          .createRemoteUser(containerManagerBindAddr);
      ugi.addToken(token);
      ugiMap.putIfAbsent(containerManagerBindAddr, ugi);

      user = ugiMap.get(containerManagerBindAddr);    
    }
    ContainerManager proxy =
        user.doAs(new PrivilegedAction<ContainerManager>() {
          @Override
          public ContainerManager run() {
            YarnRPC rpc = YarnRPC.create(getConfig());
            return (ContainerManager) rpc.getProxy(ContainerManager.class,
                NetUtils.createSocketAddr(containerManagerBindAddr),
                getConfig());
          }
        });
    return proxy;
  }

  private static class CommandTimer extends TimerTask {
    private final Thread commandThread;
    protected final ContainerLauncherEvent event;
    protected final String message;

    public CommandTimer(Thread thread, ContainerLauncherEvent event) {
      this.commandThread = thread;
      this.event = event;
      this.message = "Couldn't complete " + event.getType() + " on "
          + event.getContainerID() + "/" + event.getTaskAttemptID()
          + ". Interrupting and returning";
    }

    
    @Override
    public void run() {
      LOG.warn(this.message);
      this.commandThread.interrupt();
    }
  }

  /**
   * Setup and start the container on remote nodemanager.
   */
  private class EventProcessor implements Runnable {
    private ContainerLauncherEvent event;

    EventProcessor(ContainerLauncherEvent event) {
      this.event = event;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());

      // Load ContainerManager tokens before creating a connection.
      // TODO: Do it only once per NodeManager.
      final String containerManagerBindAddr = event.getContainerMgrAddress();
      ContainerId containerID = event.getContainerID();
      ContainerToken containerToken = event.getContainerToken();
      TaskAttemptId taskAttemptID = event.getTaskAttemptID();

      Timer timer = new Timer(true);

      switch(event.getType()) {

      case CONTAINER_REMOTE_LAUNCH:
        ContainerRemoteLaunchEvent launchEvent
            = (ContainerRemoteLaunchEvent) event;

        try {
          timer.schedule(new CommandTimer(Thread.currentThread(), event),
              nmTimeOut);

          ContainerManager proxy = getCMProxy(containerManagerBindAddr,
              containerToken);

          // Interruped during getProxy, but that didn't throw exception
          if (Thread.currentThread().isInterrupted()) {
            // The timer cancelled the command in the mean while.
            String message = "Start-container for " + event.getContainerID()
                + " got interrupted. Returning.";
            sendContainerLaunchFailedMsg(taskAttemptID, message);
            return;
          }

          // Construct the actual Container
          ContainerLaunchContext containerLaunchContext =
              launchEvent.getContainer();

          // Now launch the actual container
          StartContainerRequest startRequest = recordFactory
              .newRecordInstance(StartContainerRequest.class);
          startRequest.setContainerLaunchContext(containerLaunchContext);
          StartContainerResponse response = proxy.startContainer(startRequest);

          // container started properly. Stop the timer
          timer.cancel();
          if (Thread.currentThread().isInterrupted()) {
            // The timer cancelled the command in the mean while, but
            // startContainer didn't throw exception
            String message = "Start-container for " + event.getContainerID()
                + " got interrupted. Returning.";
            sendContainerLaunchFailedMsg(taskAttemptID, message);
            return;
          }

          ByteBuffer portInfo = response
              .getServiceResponse(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID);
          int port = -1;
          if(portInfo != null) {
            port = ShuffleHandler.deserializeMetaData(portInfo);
          }
          LOG.info("Shuffle port returned by ContainerManager for "
              + taskAttemptID + " : " + port);
          
          if(port < 0) {
            throw new IllegalStateException("Invalid shuffle port number "
                + port + " returned for " + taskAttemptID);
          }

          // after launching, send launched event to task attempt to move
          // it from ASSIGNED to RUNNING state
          context.getEventHandler().handle(
              new TaskAttemptContainerLaunchedEvent(taskAttemptID, port));
        } catch (Throwable t) {
          String message = "Container launch failed for " + containerID
              + " : " + StringUtils.stringifyException(t);
          sendContainerLaunchFailedMsg(taskAttemptID, message);
        } finally {
          timer.cancel();
        }

        break;

      case CONTAINER_REMOTE_CLEANUP:
        // We will have to remove the launch (meant "cleanup"? FIXME) event if it is still in eventQueue
        // and not yet processed
        if (eventQueue.contains(event)) {
          eventQueue.remove(event); // TODO: Any synchro needed?
          //deallocate the container
          context.getEventHandler().handle(
              new ContainerAllocatorEvent(taskAttemptID,
                  ContainerAllocator.EventType.CONTAINER_DEALLOCATE));
        } else {

          try {
            timer.schedule(new CommandTimer(Thread.currentThread(), event),
                nmTimeOut);

            ContainerManager proxy = getCMProxy(containerManagerBindAddr,
                containerToken);

            if (Thread.currentThread().isInterrupted()) {
              // The timer cancelled the command in the mean while. No need to
              // return, send cleanedup event anyways.
              LOG.info("Stop-container for " + event.getContainerID()
                  + " got interrupted.");
            } else {

              // TODO:check whether container is launched

              // kill the remote container if already launched
              StopContainerRequest stopRequest = recordFactory
                  .newRecordInstance(StopContainerRequest.class);
              stopRequest.setContainerId(event.getContainerID());
              proxy.stopContainer(stopRequest);
            }
          } catch (Throwable t) {
            // ignore the cleanup failure
            String message = "cleanup failed for container "
                + event.getContainerID() + " : "
                + StringUtils.stringifyException(t);
            context.getEventHandler()
                .handle(
                    new TaskAttemptDiagnosticsUpdateEvent(taskAttemptID,
                        message));
            LOG.warn(message);
          } finally {
            timer.cancel();
          }

          // after killing, send killed event to taskattempt
          context.getEventHandler().handle(
              new TaskAttemptEvent(event.getTaskAttemptID(),
                  TaskAttemptEventType.TA_CONTAINER_CLEANED));
        }
        break;
      }
    }
  }

  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(TaskAttemptId taskAttemptID,
      String message) {
    LOG.error(message);
    context.getEventHandler().handle(
        new TaskAttemptDiagnosticsUpdateEvent(taskAttemptID, message));
    context.getEventHandler().handle(
        new TaskAttemptEvent(taskAttemptID,
            TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED));
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }
}
