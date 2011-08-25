package org.apache.hadoop.yarn.api.impl.pb.service;

import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainerResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.proto.ContainerManager.ContainerManagerService.BlockingInterface;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainerResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class ContainerManagerPBServiceImpl implements BlockingInterface {

  private ContainerManager real;
  
  public ContainerManagerPBServiceImpl(ContainerManager impl) {
    this.real = impl;
  }

  @Override
  public GetContainerStatusResponseProto getContainerStatus(RpcController arg0,
      GetContainerStatusRequestProto proto) throws ServiceException {
    GetContainerStatusRequestPBImpl request = new GetContainerStatusRequestPBImpl(proto);
    try {
      GetContainerStatusResponse response = real.getContainerStatus(request);
      return ((GetContainerStatusResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StartContainerResponseProto startContainer(RpcController arg0,
      StartContainerRequestProto proto) throws ServiceException {
    StartContainerRequestPBImpl request = new StartContainerRequestPBImpl(proto);
    try {
      StartContainerResponse response = real.startContainer(request);
      return ((StartContainerResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StopContainerResponseProto stopContainer(RpcController arg0,
      StopContainerRequestProto proto) throws ServiceException {
    StopContainerRequestPBImpl request = new StopContainerRequestPBImpl(proto);
    try {
      StopContainerResponse response = real.stopContainer(request);
      return ((StopContainerResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

}
