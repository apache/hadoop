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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnRuntimeException;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.AbstractService;

import com.google.common.annotations.VisibleForTesting;

/**
 * <code>AMRMClientAsync</code> handles communication with the ResourceManager
 * and provides asynchronous updates on events such as container allocations and
 * completions.  It contains a thread that sends periodic heartbeats to the
 * ResourceManager.
 * 
 * It should be used by implementing a CallbackHandler:
 * <pre>
 * {@code
 * class MyCallbackHandler implements AMRMClientAsync.CallbackHandler {
 *   public void onContainersAllocated(List<Container> containers) {
 *     [run tasks on the containers]
 *   }
 *   
 *   public void onContainersCompleted(List<ContainerStatus> statuses) {
 *     [update progress, check whether app is done]
 *   }
 *   
 *   public void onNodesUpdated(List<NodeReport> updated) {}
 *   
 *   public void onReboot() {}
 * }
 * }
 * </pre>
 * 
 * The client's lifecycle should be managed similarly to the following:
 * 
 * <pre>
 * {@code
 * AMRMClientAsync asyncClient = new AMRMClientAsync(appAttId, 1000, new MyCallbackhandler());
 * asyncClient.init(conf);
 * asyncClient.start();
 * RegisterApplicationMasterResponse response = asyncClient
 *    .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
 *       appMasterTrackingUrl);
 * asyncClient.addContainerRequest(containerRequest);
 * [... wait for application to complete]
 * asyncClient.unregisterApplicationMaster(status, appMsg, trackingUrl);
 * asyncClient.stop();
 * }
 * </pre>
 */
@Unstable
@Evolving
public class AMRMClientAsync<T extends ContainerRequest> extends AbstractService {
  
  private static final Log LOG = LogFactory.getLog(AMRMClientAsync.class);
  
  private final AMRMClient<T> client;
  private final AtomicInteger heartbeatIntervalMs = new AtomicInteger();
  private final HeartbeatThread heartbeatThread;
  private final CallbackHandlerThread handlerThread;
  private final CallbackHandler handler;

  private final BlockingQueue<AllocateResponse> responseQueue;
  
  private final Object unregisterHeartbeatLock = new Object();
  
  private volatile boolean keepRunning;
  private volatile float progress;
  
  private volatile Exception savedException;
  
  public AMRMClientAsync(ApplicationAttemptId id, int intervalMs,
      CallbackHandler callbackHandler) {
    this(new AMRMClientImpl<T>(id), intervalMs, callbackHandler);
  }
  
  @Private
  @VisibleForTesting
  public AMRMClientAsync(AMRMClient<T> client, int intervalMs,
      CallbackHandler callbackHandler) {
    super(AMRMClientAsync.class.getName());
    this.client = client;
    this.heartbeatIntervalMs.set(intervalMs);
    handler = callbackHandler;
    heartbeatThread = new HeartbeatThread();
    handlerThread = new CallbackHandlerThread();
    responseQueue = new LinkedBlockingQueue<AllocateResponse>();
    keepRunning = true;
    savedException = null;
  }
    
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    client.init(conf);
  }  
  
  @Override
  protected void serviceStart() throws Exception {
    handlerThread.start();
    client.start();
    super.serviceStart();
  }
  
  /**
   * Tells the heartbeat and handler threads to stop and waits for them to
   * terminate.  Calling this method from the callback handler thread would cause
   * deadlock, and thus should be avoided.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (Thread.currentThread() == handlerThread) {
      throw new YarnRuntimeException("Cannot call stop from callback handler thread!");
    }
    keepRunning = false;
    try {
      heartbeatThread.join();
    } catch (InterruptedException ex) {
      LOG.error("Error joining with heartbeat thread", ex);
    }
    client.stop();
    try {
      handlerThread.interrupt();
      handlerThread.join();
    } catch (InterruptedException ex) {
      LOG.error("Error joining with hander thread", ex);
    }
    super.serviceStop();
  }
  
  public void setHeartbeatInterval(int interval) {
    heartbeatIntervalMs.set(interval);
  }
  
  public List<? extends Collection<T>> getMatchingRequests(
                                                   Priority priority, 
                                                   String resourceName, 
                                                   Resource capability) {
    return client.getMatchingRequests(priority, resourceName, capability);
  }
  
  /**
   * Registers this application master with the resource manager. On successful
   * registration, starts the heartbeating thread.
   * @throws YarnException
   * @throws IOException
   */
  public RegisterApplicationMasterResponse registerApplicationMaster(
      String appHostName, int appHostPort, String appTrackingUrl)
      throws YarnException, IOException {
    RegisterApplicationMasterResponse response = client
        .registerApplicationMaster(appHostName, appHostPort, appTrackingUrl);
    heartbeatThread.start();
    return response;
  }

  /**
   * Unregister the application master. This must be called in the end.
   * @param appStatus Success/Failure status of the master
   * @param appMessage Diagnostics message on failure
   * @param appTrackingUrl New URL to get master info
   * @throws YarnException
   * @throws IOException
   */
  public void unregisterApplicationMaster(FinalApplicationStatus appStatus,
      String appMessage, String appTrackingUrl) throws YarnException,
      IOException {
    synchronized (unregisterHeartbeatLock) {
      keepRunning = false;
      client.unregisterApplicationMaster(appStatus, appMessage, appTrackingUrl);
    }
  }

  /**
   * Request containers for resources before calling <code>allocate</code>
   * @param req Resource request
   */
  public void addContainerRequest(T req) {
    client.addContainerRequest(req);
  }

  /**
   * Remove previous container request. The previous container request may have 
   * already been sent to the ResourceManager. So even after the remove request 
   * the app must be prepared to receive an allocation for the previous request 
   * even after the remove request
   * @param req Resource request
   */
  public void removeContainerRequest(T req) {
    client.removeContainerRequest(req);
  }

  /**
   * Release containers assigned by the Resource Manager. If the app cannot use
   * the container or wants to give up the container then it can release them.
   * The app needs to make new requests for the released resource capability if
   * it still needs it. eg. it released non-local resources
   * @param containerId
   */
  public void releaseAssignedContainer(ContainerId containerId) {
    client.releaseAssignedContainer(containerId);
  }

  /**
   * Get the currently available resources in the cluster.
   * A valid value is available after a call to allocate has been made
   * @return Currently available resources
   */
  public Resource getClusterAvailableResources() {
    return client.getClusterAvailableResources();
  }

  /**
   * Get the current number of nodes in the cluster.
   * A valid values is available after a call to allocate has been made
   * @return Current number of nodes in the cluster
   */
  public int getClusterNodeCount() {
    return client.getClusterNodeCount();
  }
  
  private class HeartbeatThread extends Thread {
    public HeartbeatThread() {
      super("AMRM Heartbeater thread");
    }
    
    public void run() {
      while (true) {
        AllocateResponse response = null;
        // synchronization ensures we don't send heartbeats after unregistering
        synchronized (unregisterHeartbeatLock) {
          if (!keepRunning) {
            break;
          }
            
          try {
            response = client.allocate(progress);
          } catch (YarnException ex) {
            LOG.error("Yarn exception on heartbeat", ex);
            savedException = ex;
            // interrupt handler thread in case it waiting on the queue
            handlerThread.interrupt();
            break;
          } catch (IOException e) {
            LOG.error("IO exception on heartbeat", e);
            savedException = e;
            // interrupt handler thread in case it waiting on the queue
            handlerThread.interrupt();
            break;
          }
        }
        if (response != null) {
          while (true) {
            try {
              responseQueue.put(response);
              break;
            } catch (InterruptedException ex) {
              LOG.info("Interrupted while waiting to put on response queue", ex);
            }
          }
        }
        
        try {
          Thread.sleep(heartbeatIntervalMs.get());
        } catch (InterruptedException ex) {
          LOG.info("Heartbeater interrupted", ex);
        }
      }
    }
  }
  
  private class CallbackHandlerThread extends Thread {
    public CallbackHandlerThread() {
      super("AMRM Callback Handler Thread");
    }
    
    public void run() {
      while (keepRunning) {
        AllocateResponse response;
        try {
          if(savedException != null) {
            LOG.error("Stopping callback due to: ", savedException);
            handler.onError(savedException);
            break;
          }
          response = responseQueue.take();
        } catch (InterruptedException ex) {
          LOG.info("Interrupted while waiting for queue", ex);
          continue;
        }

        if (response.getAMCommand() != null) {
          boolean stop = false;
          switch(response.getAMCommand()) {
          case AM_RESYNC:
          case AM_SHUTDOWN:
            handler.onShutdownRequest();
            LOG.info("Shutdown requested. Stopping callback.");
            stop = true;
            break;
          default:
            String msg =
                  "Unhandled value of AMCommand: " + response.getAMCommand();
            LOG.error(msg);
            throw new YarnRuntimeException(msg);
          }
          if(stop) {
            // should probably stop heartbeating also YARN-763
            break;
          }
        }
        List<NodeReport> updatedNodes = response.getUpdatedNodes();
        if (!updatedNodes.isEmpty()) {
          handler.onNodesUpdated(updatedNodes);
        }
        
        List<ContainerStatus> completed =
            response.getCompletedContainersStatuses();
        if (!completed.isEmpty()) {
          handler.onContainersCompleted(completed);
        }

        List<Container> allocated = response.getAllocatedContainers();
        if (!allocated.isEmpty()) {
          handler.onContainersAllocated(allocated);
        }
        
        progress = handler.getProgress();
      }
    }
  }
  
  public interface CallbackHandler {
    
    /**
     * Called when the ResourceManager responds to a heartbeat with completed
     * containers. If the response contains both completed containers and
     * allocated containers, this will be called before containersAllocated.
     */
    public void onContainersCompleted(List<ContainerStatus> statuses);
    
    /**
     * Called when the ResourceManager responds to a heartbeat with allocated
     * containers. If the response containers both completed containers and
     * allocated containers, this will be called after containersCompleted.
     */
    public void onContainersAllocated(List<Container> containers);
    
    /**
     * Called when the ResourceManager wants the ApplicationMaster to shutdown
     * for being out of sync etc. The ApplicationMaster should not unregister
     * with the RM unless the ApplicationMaster wants to be the last attempt.
     */
    public void onShutdownRequest();
    
    /**
     * Called when nodes tracked by the ResourceManager have changed in health,
     * availability etc.
     */
    public void onNodesUpdated(List<NodeReport> updatedNodes);
    
    public float getProgress();
    
    public void onError(Exception e);
  }
}
