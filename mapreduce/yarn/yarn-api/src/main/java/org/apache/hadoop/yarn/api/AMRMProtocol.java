package org.apache.hadoop.yarn.api;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

public interface AMRMProtocol {
  public RegisterApplicationMasterResponse registerApplicationMaster(RegisterApplicationMasterRequest request) throws YarnRemoteException;
  public FinishApplicationMasterResponse finishApplicationMaster(FinishApplicationMasterRequest request) throws YarnRemoteException;;
  public AllocateResponse allocate(AllocateRequest request) throws YarnRemoteException;
}
