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
package org.apache.hadoop.yarn.server.router.subcluster;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.service.CompositeService.CompositeServiceShutdownHook;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process entry point that starts a {@link Router} for the federation
 * end-to-end suites. This is not a test: it declares no test methods.
 * {@link TestFederationSubCluster} launches it as a separate JVM and stops it
 * with Process#destroy.
 */
public class TestMockRouter {

  private static final Logger LOG = LoggerFactory.getLogger(TestMockRouter.class);

  public TestMockRouter() {
  }

  public static void main(String[] args) throws YarnException {
    if (ArrayUtils.isEmpty(args)) {
      return;
    }

    // Step1. Parse the parameters.
    String[] params = args[0].split(",");
    int pRouterClientRMPort = Integer.parseInt(params[0]);
    int pRouterAdminAddressPort = Integer.parseInt(params[1]);
    int pRouterWebAddressPort = Integer.parseInt(params[2]);
    String zkAddress = params[3];

    LOG.info("routerClientRMPort={}, routerAdminAddressPort={}, routerWebAddressPort={}, " +
        "zkAddress = {}.", pRouterClientRMPort, pRouterAdminAddressPort,
        pRouterWebAddressPort, zkAddress);

    YarnConfiguration conf = new YarnConfiguration();
    Router router = new Router();
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.set(YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS,
        "org.apache.hadoop.yarn.server.federation.store.impl.ZookeeperFederationStateStore");
    conf.set(YarnConfiguration.ROUTER_WEBAPP_INTERCEPTOR_CLASS_PIPELINE,
        "org.apache.hadoop.yarn.server.router.webapp.FederationInterceptorREST");
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        "org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor");
    conf.set(CommonConfigurationKeys.ZK_ADDRESS, zkAddress);
    conf.set(YarnConfiguration.ROUTER_RMADMIN_INTERCEPTOR_CLASS_PIPELINE,
        "org.apache.hadoop.yarn.server.router.rmadmin.FederationRMAdminInterceptor");
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, -1);
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_ADDRESS, getHostNameAndPort(pRouterClientRMPort));
    conf.set(YarnConfiguration.ROUTER_RMADMIN_ADDRESS, getHostNameAndPort(pRouterAdminAddressPort));
    conf.set(YarnConfiguration.ROUTER_WEBAPP_ADDRESS,
        getHostNameAndPort(pRouterWebAddressPort));

    // This class is launched as its own JVM by JavaProcess, and the parent test
    // terminates it with Process#destroy: SIGTERM on Linux and macOS, which runs
    // shutdown hooks. (On Windows destroy maps to TerminateProcess, which runs
    // none, so there this is inert.) Router#main registers this hook itself;
    // starting the Router directly, as here, skips it, so without this the JVM
    // exits with the Router's services still running and none of them ever
    // stopped.
    ShutdownHookManager.get().addShutdownHook(
        new CompositeServiceShutdownHook(router), Router.SHUTDOWN_HOOK_PRIORITY);
    router.init(conf);
    router.start();

    // Starting the Router has already created and initialized the facade's
    // store: RouterClientRMService#serviceStart builds a
    // RouterDelegationTokenSecretManager whose constructor calls
    // FederationStateStoreFacade#getInstance(Configuration). Building a second
    // store here and handing it to reinitialize() would orphan that first one -
    // reinitialize swaps the reference, it does not close what it replaces - so
    // take the store the facade is already using.
    FederationStateStore stateStore =
        FederationStateStoreFacade.getInstance(conf).getStateStore();

    // The facade holds this store but never closes it, so its ZooKeeper
    // connection outlives the process unless we close it ourselves. Registered
    // at a lower priority than the Router hook above: ShutdownHookManager runs
    // the highest priority first, so the Router is fully stopped before the
    // store it may still be using goes away. That ordering is only sound
    // because RouterClientRMService#serviceStop stops the delegation token
    // secret manager's ExpiredTokenRemover, which would otherwise still be
    // reaching the facade after the Router has stopped.
    ShutdownHookManager.get().addShutdownHook(() -> {
      try {
        stateStore.close();
      } catch (Exception e) {
        LOG.warn("Error closing FederationStateStore.", e);
      }
    }, Router.SHUTDOWN_HOOK_PRIORITY - 5);
  }

  private static String getHostNameAndPort(int port) {
    return MiniYARNCluster.getHostname() + ":" + port;
  }
}
