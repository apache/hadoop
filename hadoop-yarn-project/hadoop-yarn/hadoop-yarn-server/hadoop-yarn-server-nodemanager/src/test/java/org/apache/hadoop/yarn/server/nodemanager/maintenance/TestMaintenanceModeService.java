package org.apache.hadoop.yarn.server.nodemanager.maintenance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TestMaintenanceModeService {
  /**
   * Test maintenance mode service with valid config. This also tests
   * maintenance mode service e2e.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testMaintenanceModeServiceWithValidConfig()
      throws InterruptedException {

    Configuration config = getDefaultMaintenanceModeServiceTestConfig();
    testMaintenanceModeServiceWithConfig(config);
  }

  /**
   * Test maintenance mode service with null config. This test maintenance mode
   * service when we have no providers/consumers.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testMaintenanceModeServiceWithNullConfig()
      throws InterruptedException {
    Configuration conf = new Configuration();
    NodeManager nodeManager = mock(NodeManager.class);
    MaintenanceModeService ms = new MaintenanceModeService(nodeManager);
    ms.init(conf);
    ms.start();
  }

  /**
   * Test maintenance mode service with config.
   *
   * @param conf the conf
   * @throws InterruptedException the interrupted exception
   */
  private void testMaintenanceModeServiceWithConfig(Configuration conf)
      throws InterruptedException {
    MaintenanceModeService ms = new MaintenanceModeService(null);
    ms.init(conf);
    ms.start();

    // Execute the maintenance cycle 5 times
    for (int i = 0; i < 3; i++) {
      maintenanceModeCycle();
    }
    ms.stop();
  }

  /**
   * Verify maintenance mode till timeout.
   *
   * @param isOnMode         the is on mode
   * @param timeoutInSeconds the timeout in seconds
   * @throws InterruptedException the interrupted exception
   */
  private void verifymaintenanceModeTillTimeout(boolean isOnMode,
      int timeoutInSeconds) throws InterruptedException {
    int elapsedTime = 0;
    while (elapsedTime < timeoutInSeconds) {
      Assert
          .assertTrue(isOnMode == MockMaintenanceModeConsumer.isMaintenance());
      Thread.sleep(1 * 1000);
      elapsedTime++;
    }
  }

  /**
   * Gets the default maintenance mode service test configuration.
   *
   * @return the default maintenance mode service test configuration
   */
  private Configuration getDefaultMaintenanceModeServiceTestConfig() {
    Configuration conf = new Configuration();
    // Set mock provider and consumer
    conf.set(YarnConfiguration.NM_MAINTENANCE_MODE_PROVIDER_CLASSNAMES,
        "org.apache.hadoop.yarn.server.nodemanager.maintenance.MockMaintenanceModeProvider");

    conf.set(YarnConfiguration.NM_MAINTENANCE_MODE_CONSUMER_CLASSNAMES,
        "org.apache.hadoop.yarn.server.nodemanager.maintenance.MockMaintenanceModeConsumer");

    // Set heartbeat settings for producer and consumer
    conf.set(
        MaintenanceModeConfiguration.NM_MAINTENANCE_MODE_CONSUMER_HEARTBEAT_EXPIRY_SECONDS,
        "10");
    conf.set(
        MaintenanceModeConfiguration.NM_MAINTENANCE_MODE_PROVIDER_HEARTBEAT_INTERVAL_SECONDS,
        "2");
    return conf;
  }

  /**
   * Maintenance mode cycle.
   *
   * @throws InterruptedException the interrupted exception
   */
  private void maintenanceModeCycle() throws InterruptedException {
    // 1) Consumer should not be maintenance mode on init
    Assert.assertTrue(!MockMaintenanceModeConsumer.isMaintenance());

    // 2) Set maintenance mode on in mock producer.
    MockMaintenanceModeProvider.maintenance.set(true);

    // 3) Verify that the consumer has received the maintenance mode event
    waitForMaintenanceModeAndAssertOnTimeout(true, 10);

    // 4) Set maintenance mode OFF
    MockMaintenanceModeProvider.maintenance.set(false);

    // 5) Verify that the maintenanceMode is off on consumer
    waitForMaintenanceModeAndAssertOnTimeout(false, 20);

  }

  /**
   * Wait for maintenance mode and assert on timeout.
   *
   * @param isOnMode         the is on mode
   * @param timeoutInSeconds the timeout in seconds
   * @throws InterruptedException the interrupted exception
   */
  private void waitForMaintenanceModeAndAssertOnTimeout(boolean isOnMode,
      int timeoutInSeconds) throws InterruptedException {
    boolean modeSet = false;
    int elapsedTime = 0;
    while (elapsedTime < timeoutInSeconds) {
      if (isOnMode == MockMaintenanceModeConsumer.isMaintenance()) {
        modeSet = true;
        break;
      }
      Thread.sleep(1 * 1000);
      elapsedTime++;
    }
    Assert.assertTrue(modeSet);
  }
}
