package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.exceptions.YarnException;

public class SchedulerDynamicEditException extends YarnException {

  private static final long serialVersionUID = 7100374511387193257L;

  public SchedulerDynamicEditException(String string) {
    super(string);
  }

}
