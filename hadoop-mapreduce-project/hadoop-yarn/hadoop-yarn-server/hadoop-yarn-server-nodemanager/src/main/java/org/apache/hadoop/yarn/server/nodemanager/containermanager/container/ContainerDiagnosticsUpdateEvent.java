package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class ContainerDiagnosticsUpdateEvent extends ContainerEvent {

  private final String diagnosticsUpdate;

  public ContainerDiagnosticsUpdateEvent(ContainerId cID, String update) {
    super(cID, ContainerEventType.UPDATE_DIAGNOSTICS_MSG);
    this.diagnosticsUpdate = update;
  }

  public String getDiagnosticsUpdate() {
    return this.diagnosticsUpdate;
  }
}
