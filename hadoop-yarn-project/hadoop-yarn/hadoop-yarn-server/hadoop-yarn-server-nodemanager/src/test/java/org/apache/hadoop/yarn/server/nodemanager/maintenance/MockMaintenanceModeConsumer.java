package org.apache.hadoop.yarn.server.nodemanager.maintenance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class MockMaintenanceModeConsumer extends MaintenanceModeConsumer {
  private static final Logger LOG =
      LoggerFactory.getLogger(MockMaintenanceModeConsumer.class);

  public static AtomicBoolean maintenance = new AtomicBoolean(false);

  public MockMaintenanceModeConsumer(NodeManager nodeManager) {
    super(MockMaintenanceModeConsumer.class.getName(), nodeManager);
  }

  public static boolean isMaintenance() {
    return maintenance.get();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void maintenanceModeStarted() {
    LOG.info("Setting Maintenance mode ON");
    maintenance.set(true);
  }

  @Override
  protected void maintenanceModeEnded() {
    LOG.info("Maintenance mode OFF");
    maintenance.set(false);
  }
}

