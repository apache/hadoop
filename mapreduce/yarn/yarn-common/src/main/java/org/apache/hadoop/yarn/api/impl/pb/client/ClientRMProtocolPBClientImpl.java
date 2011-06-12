package org.apache.hadoop.yarn.api.impl.pb.client;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllApplicationsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllApplicationsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterMetricsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterMetricsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewApplicationIdRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewApplicationIdResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueUserAclsInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueUserAclsInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.ProtoOverHadoopRpcEngine;
import org.apache.hadoop.yarn.proto.ClientRMProtocol.ClientRMProtocolService;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationIdRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoResponseProto;

import com.google.protobuf.ServiceException;

public class ClientRMProtocolPBClientImpl implements ClientRMProtocol {

  private ClientRMProtocolService.BlockingInterface proxy;
  
  public ClientRMProtocolPBClientImpl(long clientVersion, InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ClientRMProtocolService.BlockingInterface.class, ProtoOverHadoopRpcEngine.class);
    proxy = (ClientRMProtocolService.BlockingInterface)RPC.getProxy(
        ClientRMProtocolService.BlockingInterface.class, clientVersion, addr, conf);
  }
  
  @Override
  public FinishApplicationResponse finishApplication(
      FinishApplicationRequest request) throws YarnRemoteException {
    FinishApplicationRequestProto requestProto = ((FinishApplicationRequestPBImpl)request).getProto();
    try {
      return new FinishApplicationResponsePBImpl(proxy.finishApplication(null, requestProto));
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
  public GetApplicationMasterResponse getApplicationMaster(
      GetApplicationMasterRequest request) throws YarnRemoteException {
    GetApplicationMasterRequestProto requestProto = ((GetApplicationMasterRequestPBImpl)request).getProto();
    try {
      return new GetApplicationMasterResponsePBImpl(proxy.getApplicationMaster(null, requestProto));
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
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnRemoteException {
    GetClusterMetricsRequestProto requestProto = ((GetClusterMetricsRequestPBImpl)request).getProto();
    try {
      return new GetClusterMetricsResponsePBImpl(proxy.getClusterMetrics(null, requestProto));
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
  public GetNewApplicationIdResponse getNewApplicationId(
      GetNewApplicationIdRequest request) throws YarnRemoteException {
    GetNewApplicationIdRequestProto requestProto = ((GetNewApplicationIdRequestPBImpl)request).getProto();
    try {
      return new GetNewApplicationIdResponsePBImpl(proxy.getNewApplicationId(null, requestProto));
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
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnRemoteException {
    SubmitApplicationRequestProto requestProto = ((SubmitApplicationRequestPBImpl)request).getProto();
    try {
      return new SubmitApplicationResponsePBImpl(proxy.submitApplication(null, requestProto));
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
  public GetAllApplicationsResponse getAllApplications(
      GetAllApplicationsRequest request) throws YarnRemoteException {
    GetAllApplicationsRequestProto requestProto = 
      ((GetAllApplicationsRequestPBImpl)request).getProto();
    try {
      return new GetAllApplicationsResponsePBImpl(
          proxy.getAllApplications(null, requestProto));
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
  public GetClusterNodesResponse getClusterNodes(
      GetClusterNodesRequest request) throws YarnRemoteException {
    GetClusterNodesRequestProto requestProto =
      ((GetClusterNodesRequestPBImpl)request).getProto();
    try {
      return new GetClusterNodesResponsePBImpl(
          proxy.getClusterNodes(null, requestProto));
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
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnRemoteException {
    GetQueueInfoRequestProto requestProto =
      ((GetQueueInfoRequestPBImpl)request).getProto();
    try {
      return new GetQueueInfoResponsePBImpl(
          proxy.getQueueInfo(null, requestProto));
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
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
    GetQueueUserAclsInfoRequestProto requestProto =
      ((GetQueueUserAclsInfoRequestPBImpl)request).getProto();
    try {
      return new GetQueueUserAclsInfoResponsePBImpl(
          proxy.getQueueUserAcls(null, requestProto));
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
