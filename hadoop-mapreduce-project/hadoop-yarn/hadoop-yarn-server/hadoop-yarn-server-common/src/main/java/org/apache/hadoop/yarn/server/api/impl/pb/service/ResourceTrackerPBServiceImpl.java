package org.apache.hadoop.yarn.server.api.impl.pb.service;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.proto.ResourceTracker.ResourceTrackerService.BlockingInterface;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class ResourceTrackerPBServiceImpl implements BlockingInterface {

  private ResourceTracker real;
  
  public ResourceTrackerPBServiceImpl(ResourceTracker impl) {
    this.real = impl;
  }
  
  @Override
  public RegisterNodeManagerResponseProto registerNodeManager(
      RpcController controller, RegisterNodeManagerRequestProto proto)
      throws ServiceException {
    RegisterNodeManagerRequestPBImpl request = new RegisterNodeManagerRequestPBImpl(proto);
    try {
      RegisterNodeManagerResponse response = real.registerNodeManager(request);
      return ((RegisterNodeManagerResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public NodeHeartbeatResponseProto nodeHeartbeat(RpcController controller,
      NodeHeartbeatRequestProto proto) throws ServiceException {
    NodeHeartbeatRequestPBImpl request = new NodeHeartbeatRequestPBImpl(proto);
    try {
      NodeHeartbeatResponse response = real.nodeHeartbeat(request);
      return ((NodeHeartbeatResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

}
