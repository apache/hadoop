package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

public class ContainerResourceFailedEvent extends ContainerResourceEvent {

  private final Throwable exception;

  public ContainerResourceFailedEvent(ContainerId container,
      LocalResourceRequest rsrc, Throwable cause) {
    super(container, ContainerEventType.RESOURCE_FAILED, rsrc);
    this.exception = cause;
  }

  public Throwable getCause() {
    return exception;
  }
}
