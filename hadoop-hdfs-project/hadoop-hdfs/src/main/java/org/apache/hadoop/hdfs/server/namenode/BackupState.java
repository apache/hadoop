package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.ipc.StandbyException;

@InterfaceAudience.Private
public class BackupState extends HAState {

  public BackupState() {
    super(HAServiceState.STANDBY);
  }

  @Override // HAState
  public void checkOperation(HAContext context, OperationCategory op)
      throws StandbyException {
    context.checkOperation(op);
  }

  @Override // HAState
  public boolean shouldPopulateReplQueues() {
    return false;
  }

  @Override // HAState
  public void enterState(HAContext context) throws ServiceFailedException {
    try {
      context.startActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start backup services", e);
    }
  }

  @Override // HAState
  public void exitState(HAContext context) throws ServiceFailedException {
    try {
      context.stopActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop backup services", e);
    }
  }

  @Override // HAState
  public void prepareToExitState(HAContext context) throws ServiceFailedException {
    context.prepareToStopStandbyServices();
  }
}
