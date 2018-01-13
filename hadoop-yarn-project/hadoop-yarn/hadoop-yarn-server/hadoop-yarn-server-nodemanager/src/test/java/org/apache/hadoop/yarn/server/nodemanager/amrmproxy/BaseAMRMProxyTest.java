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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerStateTransitionListener;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.NodeResourceMonitor;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredAMRMProxyState;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Base class for all the AMRMProxyService test cases. It provides utility
 * methods that can be used by the concrete test case classes
 *
 */
public abstract class BaseAMRMProxyTest {
  private static final Logger LOG =
       LoggerFactory.getLogger(BaseAMRMProxyTest.class);
  // The AMRMProxyService instance that will be used by all the test cases
  private MockAMRMProxyService amrmProxyService;

  // Thread pool used for asynchronous operations
  private static ExecutorService threadpool = Executors
      .newCachedThreadPool();
  private Configuration conf;
  private AsyncDispatcher dispatcher;
  private Context nmContext;

  protected MockAMRMProxyService getAMRMProxyService() {
    Assert.assertNotNull(this.amrmProxyService);
    return this.amrmProxyService;
  }

  @Before
  public void setUp() throws IOException {
    this.conf = createConfiguration();
    this.dispatcher = new AsyncDispatcher();
    this.dispatcher.init(this.conf);
    this.dispatcher.start();
    createAndStartAMRMProxyService(this.conf);
  }

  protected YarnConfiguration createConfiguration() {
    YarnConfiguration config = new YarnConfiguration();
    config.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
    String mockPassThroughInterceptorClass =
        PassThroughRequestInterceptor.class.getName();

    // Create a request intercepter pipeline for testing. The last one in the
    // chain will call the mock resource manager. The others in the chain will
    // simply forward it to the next one in the chain
    config.set(YarnConfiguration.AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," + mockPassThroughInterceptorClass
            + "," + mockPassThroughInterceptorClass + ","
            + MockRequestInterceptor.class.getName());

    config.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    return config;
  }

  @After
  public void tearDown() {
    this.amrmProxyService.stop();
    this.amrmProxyService = null;
    this.dispatcher.stop();
    if (this.nmContext.getNMStateStore() != null) {
      this.nmContext.getNMStateStore().stop();
    }
  }

  protected ExecutorService getThreadPool() {
    return threadpool;
  }

  protected Configuration getConf() {
    return this.conf;
  }

  protected AsyncDispatcher getDispatcher() {
    return this.dispatcher;
  }

  protected void createAndStartAMRMProxyService(Configuration config)
      throws IOException {
    // Stop the existing instance first if not null
    if (this.amrmProxyService != null) {
      this.amrmProxyService.stop();
    }
    if (this.nmContext == null) {
      this.nmContext = createContext();
    }
    this.amrmProxyService =
        new MockAMRMProxyService(this.nmContext, this.dispatcher);
    this.amrmProxyService.init(config);
    this.amrmProxyService.recover();
    this.amrmProxyService.start();
  }

  protected Context createContext() {
    NMMemoryStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(this.conf);
    stateStore.start();
    return new NMContext(null, null, null, null, stateStore, false, this.conf);
  }

  // A utility method for intercepter recover unit test
  protected Map<String, byte[]> recoverDataMapForAppAttempt(
      NMStateStoreService nmStateStore, ApplicationAttemptId attemptId)
      throws IOException {
    RecoveredAMRMProxyState state = nmStateStore.loadAMRMProxyState();
    for (Map.Entry<ApplicationAttemptId, Map<String, byte[]>> entry : state
        .getAppContexts().entrySet()) {
      if (entry.getKey().equals(attemptId)) {
        return entry.getValue();
      }
    }
    return null;
  }

  protected List<ContainerId> getCompletedContainerIds(
      List<ContainerStatus> containerStatus) {
    List<ContainerId> ret = new ArrayList<>();
    for (ContainerStatus status : containerStatus) {
      ret.add(status.getContainerId());
    }
    return ret;
  }

  /**
   * This helper method will invoke the specified function in parallel for each
   * end point in the specified list using a thread pool and return the
   * responses received from the function. It implements the logic required for
   * dispatching requests in parallel and waiting for the responses. If any of
   * the function call fails or times out, it will ignore and proceed with the
   * rest. So the responses returned can be less than the number of end points
   * specified
   * 
   * @param testContexts
   * @param func
   * @return
   */
  protected <T, R> List<R> runInParallel(List<T> testContexts,
      final Function<T, R> func) {
    ExecutorCompletionService<R> completionService =
        new ExecutorCompletionService<R>(this.getThreadPool());
    LOG.info("Sending requests to endpoints asynchronously. Number of test contexts="
        + testContexts.size());
    for (int index = 0; index < testContexts.size(); index++) {
      final T testContext = testContexts.get(index);

      LOG.info("Adding request to threadpool for test context: "
          + testContext.toString());

      completionService.submit(new Callable<R>() {
        @Override
        public R call() throws Exception {
          LOG.info("Sending request. Test context:"
              + testContext.toString());

          R response = null;
          try {
            response = func.invoke(testContext);
            LOG.info("Successfully sent request for context: "
                + testContext.toString());
          } catch (Throwable ex) {
            LOG.error("Failed to process request for context: "
                + testContext);
            response = null;
          }

          return response;
        }
      });
    }

    ArrayList<R> responseList = new ArrayList<R>();
    LOG.info("Waiting for responses from endpoints. Number of contexts="
        + testContexts.size());
    for (int i = 0; i < testContexts.size(); ++i) {
      try {
        final Future<R> future = completionService.take();
        final R response = future.get(3000, TimeUnit.MILLISECONDS);
        responseList.add(response);
      } catch (Throwable e) {
        LOG.error("Failed to process request " + e.getMessage());
      }
    }

    return responseList;
  }

  /**
   * Helper method to register an application master using specified testAppId
   * as the application identifier and return the response
   * 
   * @param testAppId
   * @return
   * @throws Exception
   * @throws YarnException
   * @throws IOException
   */
  protected RegisterApplicationMasterResponse registerApplicationMaster(
      final int testAppId) throws Exception, YarnException, IOException {
    final ApplicationUserInfo ugi = getApplicationUserInfo(testAppId);

    return ugi
        .getUser()
        .doAs(
            new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
              @Override
              public RegisterApplicationMasterResponse run()
                  throws Exception {
                getAMRMProxyService().initApp(
                    ugi.getAppAttemptId(),
                    ugi.getUser().getUserName());

                final RegisterApplicationMasterRequest req =
                    Records
                        .newRecord(RegisterApplicationMasterRequest.class);
                req.setHost(Integer.toString(testAppId));
                req.setRpcPort(testAppId);
                req.setTrackingUrl("");

                RegisterApplicationMasterResponse response =
                    getAMRMProxyService().registerApplicationMaster(req);
                return response;
              }
            });
  }

  /**
   * Helper method that can be used to register multiple application masters in
   * parallel to the specified RM end points
   * 
   * @param testContexts - used to identify the requests
   * @return
   */
  protected <T> List<RegisterApplicationMasterResponseInfo<T>> registerApplicationMastersInParallel(
      final ArrayList<T> testContexts) {
    List<RegisterApplicationMasterResponseInfo<T>> responses =
        runInParallel(testContexts,
            new Function<T, RegisterApplicationMasterResponseInfo<T>>() {
              @Override
              public RegisterApplicationMasterResponseInfo<T> invoke(
                  T testContext) {
                RegisterApplicationMasterResponseInfo<T> response = null;
                try {
                  int index = testContexts.indexOf(testContext);
                  response =
                      new RegisterApplicationMasterResponseInfo<T>(
                          registerApplicationMaster(index), testContext);
                  Assert.assertNotNull(response.getResponse());
                  Assert.assertEquals(Integer.toString(index), response
                      .getResponse().getQueue());

                  LOG.info("Sucessfully registered application master with test context: "
                      + testContext);
                } catch (Throwable ex) {
                  response = null;
                  LOG.error("Failed to register application master with test context: "
                      + testContext);
                }

                return response;
              }
            });

    Assert.assertEquals(
        "Number of responses received does not match with request",
        testContexts.size(), responses.size());

    Set<T> contextResponses = new TreeSet<T>();
    for (RegisterApplicationMasterResponseInfo<T> item : responses) {
      contextResponses.add(item.getTestContext());
    }

    for (T ep : testContexts) {
      Assert.assertTrue(contextResponses.contains(ep));
    }

    return responses;
  }

  /**
   * Unregisters the application master for specified application id
   * 
   * @param appId
   * @param status
   * @return
   * @throws Exception
   * @throws YarnException
   * @throws IOException
   */
  protected FinishApplicationMasterResponse finishApplicationMaster(
      final int appId, final FinalApplicationStatus status)
      throws Exception, YarnException, IOException {

    final ApplicationUserInfo ugi = getApplicationUserInfo(appId);

    return ugi.getUser().doAs(
        new PrivilegedExceptionAction<FinishApplicationMasterResponse>() {
          @Override
          public FinishApplicationMasterResponse run() throws Exception {
            final FinishApplicationMasterRequest req =
                Records.newRecord(FinishApplicationMasterRequest.class);
            req.setDiagnostics("");
            req.setTrackingUrl("");
            req.setFinalApplicationStatus(status);

            FinishApplicationMasterResponse response =
                getAMRMProxyService().finishApplicationMaster(req);

            getAMRMProxyService().stopApp(
                ugi.getAppAttemptId().getApplicationId());

            return response;
          }
        });
  }

  protected <T> List<FinishApplicationMasterResponseInfo<T>> finishApplicationMastersInParallel(
      final ArrayList<T> testContexts) {
    List<FinishApplicationMasterResponseInfo<T>> responses =
        runInParallel(testContexts,
            new Function<T, FinishApplicationMasterResponseInfo<T>>() {
              @Override
              public FinishApplicationMasterResponseInfo<T> invoke(
                  T testContext) {
                FinishApplicationMasterResponseInfo<T> response = null;
                try {
                  response =
                      new FinishApplicationMasterResponseInfo<T>(
                          finishApplicationMaster(
                              testContexts.indexOf(testContext),
                              FinalApplicationStatus.SUCCEEDED),
                          testContext);
                  Assert.assertNotNull(response.getResponse());

                  LOG.info("Sucessfully finished application master with test contexts: "
                      + testContext);
                } catch (Throwable ex) {
                  response = null;
                  LOG.error("Failed to finish application master with test context: "
                      + testContext);
                }

                return response;
              }
            });

    Assert.assertEquals(
        "Number of responses received does not match with request",
        testContexts.size(), responses.size());

    Set<T> contextResponses = new TreeSet<T>();
    for (FinishApplicationMasterResponseInfo<T> item : responses) {
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getResponse());
      contextResponses.add(item.getTestContext());
    }

    for (T ep : testContexts) {
      Assert.assertTrue(contextResponses.contains(ep));
    }

    return responses;
  }

  protected AllocateResponse allocate(final int testAppId)
      throws Exception, YarnException, IOException {
    final AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(testAppId);
    return allocate(testAppId, req);
  }

  protected AllocateResponse allocate(final int testAppId,
      final AllocateRequest request) throws Exception, YarnException,
      IOException {

    final ApplicationUserInfo ugi = getApplicationUserInfo(testAppId);

    return ugi.getUser().doAs(
        new PrivilegedExceptionAction<AllocateResponse>() {
          @Override
          public AllocateResponse run() throws Exception {
            AllocateResponse response =
                getAMRMProxyService().allocate(request);
            return response;
          }
        });
  }

  protected ApplicationUserInfo getApplicationUserInfo(final int testAppId) {
    final ApplicationAttemptId attemptId =
        getApplicationAttemptId(testAppId);

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(attemptId.toString());
    AMRMTokenIdentifier token = new AMRMTokenIdentifier(attemptId, 1);
    ugi.addTokenIdentifier(token);
    return new ApplicationUserInfo(ugi, attemptId);
  }

  protected List<ResourceRequest> createResourceRequests(String[] hosts,
      int memory, int vCores, int priority, int containers)
      throws Exception {
    return createResourceRequests(hosts, memory, vCores, priority,
        containers, null);
  }

  protected List<ResourceRequest> createResourceRequests(String[] hosts,
      int memory, int vCores, int priority, int containers,
      String labelExpression) throws Exception {
    List<ResourceRequest> reqs = new ArrayList<ResourceRequest>();
    for (String host : hosts) {
      ResourceRequest hostReq =
          createResourceRequest(host, memory, vCores, priority,
              containers, labelExpression);
      reqs.add(hostReq);
      ResourceRequest rackReq =
          createResourceRequest("/default-rack", memory, vCores, priority,
              containers, labelExpression);
      reqs.add(rackReq);
    }

    ResourceRequest offRackReq =
        createResourceRequest(ResourceRequest.ANY, memory, vCores,
            priority, containers, labelExpression);
    reqs.add(offRackReq);
    return reqs;
  }

  protected ResourceRequest createResourceRequest(String resource,
      int memory, int vCores, int priority, int containers)
      throws Exception {
    return createResourceRequest(resource, memory, vCores, priority,
        containers, null);
  }

  protected ResourceRequest createResourceRequest(String resource,
      int memory, int vCores, int priority, int containers,
      String labelExpression) throws Exception {
    ResourceRequest req = Records.newRecord(ResourceRequest.class);
    req.setResourceName(resource);
    req.setNumContainers(containers);
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(priority);
    req.setPriority(pri);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemorySize(memory);
    capability.setVirtualCores(vCores);
    req.setCapability(capability);
    if (labelExpression != null) {
      req.setNodeLabelExpression(labelExpression);
    }
    return req;
  }

  /**
   * Returns an ApplicationId with the specified identifier
   * 
   * @param testAppId
   * @return
   */
  protected ApplicationId getApplicationId(int testAppId) {
    return ApplicationId.newInstance(123456, testAppId);
  }

  /**
   * Return an instance of ApplicationAttemptId using specified identifier. This
   * identifier will be used for the ApplicationId too.
   * 
   * @param testAppId
   * @return
   */
  protected ApplicationAttemptId getApplicationAttemptId(int testAppId) {
    return ApplicationAttemptId.newInstance(getApplicationId(testAppId),
        testAppId);
  }

  /**
   * Return an instance of ApplicationAttemptId using specified identifier and
   * application id
   * 
   * @param testAppId
   * @return
   */
  protected ApplicationAttemptId getApplicationAttemptId(int testAppId,
      ApplicationId appId) {
    return ApplicationAttemptId.newInstance(appId, testAppId);
  }

  protected static class RegisterApplicationMasterResponseInfo<T> {
    private RegisterApplicationMasterResponse response;
    private T testContext;

    RegisterApplicationMasterResponseInfo(
        RegisterApplicationMasterResponse response, T testContext) {
      this.response = response;
      this.testContext = testContext;
    }

    public RegisterApplicationMasterResponse getResponse() {
      return response;
    }

    public T getTestContext() {
      return testContext;
    }
  }

  protected static class FinishApplicationMasterResponseInfo<T> {
    private FinishApplicationMasterResponse response;
    private T testContext;

    FinishApplicationMasterResponseInfo(
        FinishApplicationMasterResponse response, T testContext) {
      this.response = response;
      this.testContext = testContext;
    }

    public FinishApplicationMasterResponse getResponse() {
      return response;
    }

    public T getTestContext() {
      return testContext;
    }
  }

  protected static class ApplicationUserInfo {
    private UserGroupInformation user;
    private ApplicationAttemptId attemptId;

    ApplicationUserInfo(UserGroupInformation user,
        ApplicationAttemptId attemptId) {
      this.user = user;
      this.attemptId = attemptId;
    }

    public UserGroupInformation getUser() {
      return this.user;
    }

    public ApplicationAttemptId getAppAttemptId() {
      return this.attemptId;
    }
  }

  protected static class MockAMRMProxyService extends AMRMProxyService {
    public MockAMRMProxyService(Context nmContext,
        AsyncDispatcher dispatcher) {
      super(nmContext, dispatcher);
    }

    @Override
    protected void serviceStart() throws Exception {
      // Override this method and do nothing to avoid the base class from
      // listening to server end point
      getSecretManager().start();
    }

    /**
     * This method is used by the test code to initialize the pipeline. In the
     * actual service, the initialization is called by the
     * ContainerManagerImpl::StartContainers method
     * 
     * @param applicationId
     * @param user
     */
    public void initApp(ApplicationAttemptId applicationId, String user) {
      super.initializePipeline(applicationId, user,
          new Token<AMRMTokenIdentifier>(), null, null, false, null);
    }

    public void stopApp(ApplicationId applicationId) {
      super.stopApplication(applicationId);
    }
  }

  /**
   * The Function interface is used for passing method pointers that can be
   * invoked asynchronously at a later point.
   */
  protected interface Function<T, R> {
    public R invoke(T input);
  }

  protected class NullContext implements Context {

    @Override
    public NodeId getNodeId() {
      return null;
    }

    @Override
    public int getHttpPort() {
      return 0;
    }

    @Override
    public ConcurrentMap<ApplicationId, Application> getApplications() {
      return null;
    }

    @Override
    public Map<ApplicationId, Credentials> getSystemCredentialsForApps() {
      return null;
    }

    @Override
    public ConcurrentMap<ApplicationId, AppCollectorData>
        getRegisteringCollectors() {
      return null;
    }

    @Override
    public ConcurrentMap<ApplicationId, AppCollectorData> getKnownCollectors() {
      return null;
    }

    @Override
    public ConcurrentMap<ContainerId, Container> getContainers() {
      return null;
    }

    @Override
    public ConcurrentMap<ContainerId, org.apache.hadoop.yarn.api.records.Container> getIncreasedContainers() {
      return null;
    }

    @Override
    public NMContainerTokenSecretManager getContainerTokenSecretManager() {
      return null;
    }

    @Override
    public NMTokenSecretManagerInNM getNMTokenSecretManager() {
      return null;
    }

    @Override
    public NodeHealthStatus getNodeHealthStatus() {
      return null;
    }

    @Override
    public ContainerManager getContainerManager() {
      return null;
    }

    @Override
    public LocalDirsHandlerService getLocalDirsHandler() {
      return null;
    }

    @Override
    public ApplicationACLsManager getApplicationACLsManager() {
      return null;
    }

    @Override
    public NMStateStoreService getNMStateStore() {
      return null;
    }

    @Override
    public boolean getDecommissioned() {
      return false;
    }

    @Override
    public Configuration getConf() {
      return null;
    }

    @Override
    public void setDecommissioned(boolean isDecommissioned) {
    }

    @Override
    public ConcurrentLinkedQueue<LogAggregationReport> getLogAggregationStatusForApps() {
      return null;
    }

    @Override
    public NodeResourceMonitor getNodeResourceMonitor() {
      return null;
    }

    @Override
    public NodeStatusUpdater getNodeStatusUpdater() {
      return null;
    }

    public boolean isDistributedSchedulingEnabled() {
      return false;
    }

    @Override
    public OpportunisticContainerAllocator getContainerAllocator() {
      return null;
    }

    public void setNMTimelinePublisher(NMTimelinePublisher nmMetricsPublisher) {
    }

    @Override
    public NMTimelinePublisher getNMTimelinePublisher() {
      return  null;
    }

    @Override
    public ContainerExecutor getContainerExecutor() {
      return null;
    }

    @Override
    public ContainerStateTransitionListener
        getContainerStateTransitionListener() {
      return null;
    }

    public ResourcePluginManager getResourcePluginManager() {
      return null;
    }

    @Override
    public NodeManagerMetrics getNodeManagerMetrics() {
      return null;
    }

    @Override
    public DeletionService getDeletionService() {
      return null;
    }
  }
}
