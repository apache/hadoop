package org.apache.hadoop.yarn.server.router.ssproxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.FederationStateStoreBaseTest;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.federation.store.impl.HttpProxyFederationStateStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Validate the correctness of the StateStoreProxy in the router.
 * This test case works by setting the facade to use a MemoryStateStore,
 * and then testing the HttpProxyFederationStateStore, which communicates
 * via REST with the StateStoreProxyWebServices
 */
public class TestRouterStateStoreProxy extends FederationStateStoreBaseTest {
  private static Router router;

  private static FederationStateStore proxyStore;

  public TestRouterStateStoreProxy() {
  }

  @BeforeClass
  public static void setUpProxy() {
    // Set up the router proxy to use a memory state store
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS,
        MemoryFederationStateStore.class.getName());
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);
    conf.set(YarnConfiguration.ROUTER_SSPROXY_INTERCEPTOR_CLASS_PIPELINE,
        StateStoreProxyDefaultInterceptor.class.getName());
    proxyStore = new MemoryFederationStateStore();
    FederationStateStoreFacade.getInstance().reinitialize(proxyStore, conf);
    router = new Router();
    router.init(conf);
    router.start();
  }

  @AfterClass
  public static void tearDownProxy() {
    router.stop();
  }

  @Before @Override
  public void before() throws IOException, YarnException {
    super.before();
    proxyStore.init(getConf());
  }

  @After @Override
  public void after() throws Exception {
    super.after();
    proxyStore.close();
  }

  @Override
  protected FederationStateStore createStateStore() {
    Configuration conf = new Configuration();

    // Set up our local state store
    super.setConf(conf);
    return new HttpProxyFederationStateStore();
  }
}

