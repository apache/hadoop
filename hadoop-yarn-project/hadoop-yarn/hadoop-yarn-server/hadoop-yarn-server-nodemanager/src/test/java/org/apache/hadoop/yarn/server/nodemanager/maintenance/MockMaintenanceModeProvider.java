package org.apache.hadoop.yarn.server.nodemanager.maintenance;

import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MockMaintenanceModeProvider extends MaintenanceModeProvider {
  private final ScheduledExecutorService taskExecutor =
      Executors.newSingleThreadScheduledExecutor();
  public static AtomicBoolean maintenance = new AtomicBoolean(false);

  public MockMaintenanceModeProvider(NodeManager nodeManager,
      Dispatcher dispatcher) {
    super(MockMaintenanceModeProvider.class.getName(), nodeManager, dispatcher);

  }

  @Override
  public void start() {
    super.start();
    taskExecutor.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        if (maintenance.get()) {
          startMaintenanceMode();
        } else {
          stopMaintenanceMode();
        }

      }
    }, 0, 1, TimeUnit.SECONDS);
  }

  @Override
  synchronized public void startMaintenanceMode() {
    super.startMaintenanceMode();
  }

  @Override
  synchronized public void stopMaintenanceMode() {
    super.stopMaintenanceMode();
  }
}
