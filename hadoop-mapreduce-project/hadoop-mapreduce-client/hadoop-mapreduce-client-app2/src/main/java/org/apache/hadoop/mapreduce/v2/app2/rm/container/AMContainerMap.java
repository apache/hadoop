package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.mapreduce.jobhistory.ContainerHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;

// TODO Extending concurrentHashMap seems weird. May be simpler to just have some 
// simple methods. The iterator is kindof useful though.
@SuppressWarnings("serial")
public class AMContainerMap extends ConcurrentHashMap<ContainerId, AMContainer>
    implements EventHandler<AMContainerEvent> {

  private final ContainerHeartbeatHandler chh;
  private final TaskAttemptListener tal;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final AppContext context;

  @SuppressWarnings("rawtypes")
  public AMContainerMap(ContainerHeartbeatHandler chh, TaskAttemptListener tal,
      EventHandler eventHandler, AppContext context) {
    this.chh = chh;
    this.tal = tal;
    this.eventHandler = eventHandler;
    this.context = context;
  }

  @Override
  public void handle(AMContainerEvent event) {
    ContainerId containerId = event.getContainerId();
    get(containerId).handle(event);
  }

  public void addNewContainer(Container container) {
    AMContainer amc = new AMContainerImpl(container, chh, tal, eventHandler,
        context);
    put(container.getId(), amc);
  }
}