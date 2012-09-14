package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.jobhistory.ContainerHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

public class AMContainerMap extends AbstractService
    implements EventHandler<AMContainerEvent> {

  private static final Log LOG = LogFactory.getLog(AMContainerMap.class);
  
  private final ContainerHeartbeatHandler chh;
  private final TaskAttemptListener tal;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final AppContext context;
  private final ConcurrentHashMap<ContainerId, AMContainer>containerMap;

  @SuppressWarnings("rawtypes")
  public AMContainerMap(ContainerHeartbeatHandler chh, TaskAttemptListener tal,
      EventHandler eventHandler, AppContext context) {
    super("AMContainerMaps");
    this.chh = chh;
    this.tal = tal;
    this.eventHandler = eventHandler;
    this.context = context;
    this.containerMap = new ConcurrentHashMap<ContainerId, AMContainer>();
  }

  @Override
  public void handle(AMContainerEvent event) {
    ContainerId containerId = event.getContainerId();
    containerMap.get(containerId).handle(event);
  }

  public void addContainerIfNew(Container container) {
    AMContainer amc = new AMContainerImpl(container, chh, tal, eventHandler,
        context);
    containerMap.putIfAbsent(container.getId(), amc);
  }
  
  public AMContainer get(ContainerId containerId) {
    return containerMap.get(containerId);
  }
  
  public Collection<AMContainer>values() {
    return containerMap.values();
  }
}