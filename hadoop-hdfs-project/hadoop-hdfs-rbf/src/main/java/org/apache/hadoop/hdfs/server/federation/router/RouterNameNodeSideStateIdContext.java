package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.FederatedNamespaceIds;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.hdfs.server.federation.router.RouterStateIdCache.UniqueCallID;

import java.io.IOException;

public class RouterNameNodeSideStateIdContext implements AlignmentContext {

  private String nsId;

  RouterNameNodeSideStateIdContext(String nsId) {
    this.nsId = nsId;
  }

  @Override
  public void updateResponseState(RpcHeaderProtos.RpcResponseHeaderProto.Builder header) {
    throw new UnsupportedOperationException("Router rpc Client should not update response state");
  }

  @Override
  public void receiveResponseState(RpcHeaderProtos.RpcResponseHeaderProto header) {
    // Receive from NameNode, then update the cached variable.
    UniqueCallID uid = new UniqueCallID(header.getClientId().toByteArray(), header.getCallId());
    FederatedNamespaceIds ids = RouterStateIdCache.get(uid);
    if (ids != null) {
      ids.updateNameserviceState(nsId, header.getStateId());
      if (ids.isProxyMode()) {
        RouterStateIdCache.get(nsId).updateNameserviceState(nsId, header.getStateId());
      } else if (ids.isTransmissionMode()) {
        ids.updateNameserviceState(nsId, header.getStateId());
        RouterStateIdCache.remove(uid);
      }
    }
  }

  @Override
  public void updateRequestState(RpcHeaderProtos.RpcRequestHeaderProto.Builder header) {
    // Fill header with the cached thread local variable from client.
    // Then send to NameNode from router.
    Server.Call call = Server.getCurCall().get();
    assert call != null;
    UniqueCallID suid = new UniqueCallID(call.getClientId(), call.getCallId());
    FederatedNamespaceIds ids = RouterStateIdCache.get(suid);
    if (ids != null) {
      if (ids.isProxyMode()) {
        RouterStateIdCache.get(nsId).setRequestHeaderState(header, nsId);
      } else if (ids.isTransmissionMode()) {
        ids.setRequestHeaderState(header, nsId);
      }
      // Update to newCallId, it is used for receiveResponseState to find FederatedNamespaceIds
      UniqueCallID clientCallId = new UniqueCallID(header.getClientId().toByteArray(),
          header.getCallId());
      RouterStateIdCache.put(clientCallId, ids);
    } else {
      // If rpc request from old version hdfs client, ids will be null.
      // Then we need to disable observe read.
      header.clearStateId();
    }
  }

  @Override
  public long receiveRequestState(RpcHeaderProtos.RpcRequestHeaderProto header, long threshold,
                                  boolean isCoordinatedCall) throws IOException {
    throw new UnsupportedOperationException("Router rpc Client should not receive request state");
  }

  @Override
  public long getLastSeenStateId() {
    return 0;
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String method) {
    throw new UnsupportedOperationException("Client should not be checking uncoordinated call");
  }
}
