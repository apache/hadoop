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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * Registers/unregisters to RM and sends heartbeats to RM.
 */
public abstract class RMCommunicator extends AbstractService
    implements RMHeartbeatHandler {
  private static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);
  private int rmPollInterval;//millis
  protected ApplicationId applicationId;
  protected ApplicationAttemptId applicationAttemptId;
  private final AtomicBoolean stopped;
  protected Thread allocatorThread;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;
  protected AMRMProtocol scheduler;
  private final ClientService clientService;
  protected int lastResponseID;
  private Resource minContainerCapability;
  private Resource maxContainerCapability;
  protected Map<ApplicationAccessType, String> applicationACLs;
  private volatile long lastHeartbeatTime;
  private ConcurrentLinkedQueue<Runnable> heartbeatCallbacks;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private final AppContext context;
  private Job job;
  // Has a signal (SIGTERM etc) been issued?
  protected volatile boolean isSignalled = false;
  private volatile boolean shouldUnregister = true;

  public RMCommunicator(ClientService clientService, AppContext context) {
    super("RMCommunicator");
    this.clientService = clientService;
    this.context = context;
    this.eventHandler = context.getEventHandler();
    this.applicationId = context.getApplicationID();
    this.applicationAttemptId = context.getApplicationAttemptId();
    this.stopped = new AtomicBoolean(false);
    this.heartbeatCallbacks = new ConcurrentLinkedQueue<Runnable>();
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    rmPollInterval =
        conf.getInt(MRJobConfig.MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS,
            MRJobConfig.DEFAULT_MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS);
  }

  @Override
  public void start() {
    scheduler= createSchedulerProxy();
    register();
    startAllocatorThread();
    JobID id = TypeConverter.fromYarn(this.applicationId);
    JobId jobId = TypeConverter.toYarn(id);
    job = context.getJob(jobId);
    super.start();
  }

  protected AppContext getContext() {
    return context;
  }

  protected Job getJob() {
    return job;
  }

  /**
   * Get the appProgress. Can be used only after this component is started.
   * @return the appProgress.
   */
  protected float getApplicationProgress() {
    // For now just a single job. In future when we have a DAG, we need an
    // aggregate progress.
    return this.job.getProgress();
  }

  protected void register() {
    //Register
    InetSocketAddress serviceAddr = null;
    if (clientService != null ) {
      serviceAddr = clientService.getBindAddress();
    }
    try {
      RegisterApplicationMasterRequest request =
        recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
      request.setApplicationAttemptId(applicationAttemptId);
      if (serviceAddr != null) {
        request.setHost(serviceAddr.getHostName());
        request.setRpcPort(serviceAddr.getPort());
        request.setTrackingUrl(serviceAddr.getHostName() + ":" + clientService.getHttpPort());
      }
      RegisterApplicationMasterResponse response =
        scheduler.registerApplicationMaster(request);
      minContainerCapability = response.getMinimumResourceCapability();
      maxContainerCapability = response.getMaximumResourceCapability();
      this.context.getClusterInfo().setMinContainerCapability(
          minContainerCapability);
      this.context.getClusterInfo().setMaxContainerCapability(
          maxContainerCapability);
      this.applicationACLs = response.getApplicationACLs();
      LOG.info("minContainerCapability: " + minContainerCapability.getMemory());
      LOG.info("maxContainerCapability: " + maxContainerCapability.getMemory());
    } catch (Exception are) {
      LOG.error("Exception while registering", are);
      throw new YarnException(are);
    }
  }

  protected void unregister() {
    try {
      FinalApplicationStatus finishState = FinalApplicationStatus.UNDEFINED;
      JobImpl jobImpl = (JobImpl)job;
      if (jobImpl.getInternalState() == JobStateInternal.SUCCEEDED) {
        finishState = FinalApplicationStatus.SUCCEEDED;
      } else if (jobImpl.getInternalState() == JobStateInternal.KILLED
          || (jobImpl.getInternalState() == JobStateInternal.RUNNING && isSignalled)) {
        finishState = FinalApplicationStatus.KILLED;
      } else if (jobImpl.getInternalState() == JobStateInternal.FAILED
          || jobImpl.getInternalState() == JobStateInternal.ERROR) {
        finishState = FinalApplicationStatus.FAILED;
      }
      StringBuffer sb = new StringBuffer();
      for (String s : job.getDiagnostics()) {
        sb.append(s).append("\n");
      }
      LOG.info("Setting job diagnostics to " + sb.toString());

      String historyUrl = JobHistoryUtils.getHistoryUrl(getConfig(),
          context.getApplicationID());
      LOG.info("History url is " + historyUrl);

      FinishApplicationMasterRequest request =
          recordFactory.newRecordInstance(FinishApplicationMasterRequest.class);
      request.setAppAttemptId(this.applicationAttemptId);
      request.setFinishApplicationStatus(finishState);
      request.setDiagnostics(sb.toString());
      request.setTrackingUrl(historyUrl);
      scheduler.finishApplicationMaster(request);
    } catch(Exception are) {
      LOG.error("Exception while unregistering ", are);
    }
  }

  protected Resource getMinContainerCapability() {
    return minContainerCapability;
  }

  protected Resource getMaxContainerCapability() {
    return maxContainerCapability;
  }

  @Override
  public void stop() {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    allocatorThread.interrupt();
    try {
      allocatorThread.join();
    } catch (InterruptedException ie) {
      LOG.warn("InterruptedException while stopping", ie);
    }
    if(shouldUnregister) {
      unregister();
    }
    super.stop();
  }

  protected void startAllocatorThread() {
    allocatorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(rmPollInterval);
            try {
              heartbeat();
            } catch (YarnException e) {
              LOG.error("Error communicating with RM: " + e.getMessage() , e);
              return;
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM. ", e);
              continue;
              // TODO: for other exceptions
            }

            lastHeartbeatTime = context.getClock().getTime();
            executeHeartbeatCallbacks();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Allocated thread interrupted. Returning.");
            }
            return;
          }
        }
      }
    });
    allocatorThread.setName("RMCommunicator Allocator");
    allocatorThread.start();
  }

  protected AMRMProtocol createSchedulerProxy() {
    final Configuration conf = getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);
    final InetSocketAddress serviceAddr = conf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnException(e);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      String tokenURLEncodedStr = System.getenv().get(
          ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
      Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();

      try {
        token.decodeFromUrlString(tokenURLEncodedStr);
      } catch (IOException e) {
        throw new YarnException(e);
      }

      SecurityUtil.setTokenService(token, serviceAddr);
      if (LOG.isDebugEnabled()) {
        LOG.debug("AppMasterToken is " + token);
      }
      currentUser.addToken(token);
    }

    return currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
      @Override
      public AMRMProtocol run() {
        return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
            serviceAddr, conf);
      }
    });
  }

  protected abstract void heartbeat() throws Exception;

  private void executeHeartbeatCallbacks() {
    Runnable callback = null;
    while ((callback = heartbeatCallbacks.poll()) != null) {
      callback.run();
    }
  }

  @Override
  public long getLastHeartbeatTime() {
    return lastHeartbeatTime;
  }

  @Override
  public void runOnNextHeartbeat(Runnable callback) {
    heartbeatCallbacks.add(callback);
  }

  public void setShouldUnregister(boolean shouldUnregister) {
    this.shouldUnregister = shouldUnregister;
    LOG.info("RMCommunicator notified that shouldUnregistered is: " 
        + shouldUnregister);
  }
  
  public void setSignalled(boolean isSignalled) {
    this.isSignalled = isSignalled;
    LOG.info("RMCommunicator notified that iSignalled is: " 
        + isSignalled);
  }
}
