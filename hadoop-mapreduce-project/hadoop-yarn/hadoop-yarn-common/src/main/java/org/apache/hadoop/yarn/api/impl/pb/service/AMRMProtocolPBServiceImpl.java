package org.apache.hadoop.yarn.api.impl.pb.service;

import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.proto.AMRMProtocol.AMRMProtocolService.BlockingInterface;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationMasterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class AMRMProtocolPBServiceImpl implements BlockingInterface {

  private AMRMProtocol real;
  
  public AMRMProtocolPBServiceImpl(AMRMProtocol impl) {
    this.real = impl;
  }
  
  @Override
  public AllocateResponseProto allocate(RpcController arg0,
      AllocateRequestProto proto) throws ServiceException {
    AllocateRequestPBImpl request = new AllocateRequestPBImpl(proto);
    try {
      AllocateResponse response = real.allocate(request);
      return ((AllocateResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FinishApplicationMasterResponseProto finishApplicationMaster(
      RpcController arg0, FinishApplicationMasterRequestProto proto)
      throws ServiceException {
    FinishApplicationMasterRequestPBImpl request = new FinishApplicationMasterRequestPBImpl(proto);
    try {
      FinishApplicationMasterResponse response = real.finishApplicationMaster(request);
      return ((FinishApplicationMasterResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RegisterApplicationMasterResponseProto registerApplicationMaster(
      RpcController arg0, RegisterApplicationMasterRequestProto proto)
      throws ServiceException {
    RegisterApplicationMasterRequestPBImpl request = new RegisterApplicationMasterRequestPBImpl(proto);
    try {
      RegisterApplicationMasterResponse response = real.registerApplicationMaster(request);
      return ((RegisterApplicationMasterResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }
}
