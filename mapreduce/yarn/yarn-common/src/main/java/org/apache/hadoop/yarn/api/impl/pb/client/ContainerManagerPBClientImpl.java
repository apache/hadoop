package org.apache.hadoop.yarn.api.impl.pb.client;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainerResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.ProtoOverHadoopRpcEngine;
import org.apache.hadoop.yarn.proto.ContainerManager.ContainerManagerService;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainerRequestProto;

import com.google.protobuf.ServiceException;

public class ContainerManagerPBClientImpl implements ContainerManager {

  private ContainerManagerService.BlockingInterface proxy;
  
  public ContainerManagerPBClientImpl(long clientVersion, InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ContainerManagerService.BlockingInterface.class, ProtoOverHadoopRpcEngine.class);
    proxy = (ContainerManagerService.BlockingInterface)RPC.getProxy(
        ContainerManagerService.BlockingInterface.class, clientVersion, addr, conf);
  }

  @Override
  public GetContainerStatusResponse getContainerStatus(
      GetContainerStatusRequest request) throws YarnRemoteException {
    GetContainerStatusRequestProto requestProto = ((GetContainerStatusRequestPBImpl)request).getProto();
    try {
      return new GetContainerStatusResponsePBImpl(proxy.getContainerStatus(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public StartContainerResponse startContainer(StartContainerRequest request)
      throws YarnRemoteException {
    StartContainerRequestProto requestProto = ((StartContainerRequestPBImpl)request).getProto();
    try {
      return new StartContainerResponsePBImpl(proxy.startContainer(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public StopContainerResponse stopContainer(StopContainerRequest request)
      throws YarnRemoteException {
    StopContainerRequestProto requestProto = ((StopContainerRequestPBImpl)request).getProto();
    try {
      return new StopContainerResponsePBImpl(proxy.stopContainer(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

}
