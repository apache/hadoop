package org.apache.hadoop.yarn;

import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.junit.Test;

public class TestRPCFactories {
  
  
  
  @Test
  public void test() {
    testPbServerFactory();
    
    testPbClientFactory();
  }
  
  
  
  private void testPbServerFactory() {
    InetSocketAddress addr = new InetSocketAddress(0);
    Configuration conf = new Configuration();
    ResourceTracker instance = new ResourceTrackerTestImpl();
    Server server = null;
    try {
      server = 
        RpcServerFactoryPBImpl.get().getServer(
            ResourceTracker.class, instance, addr, conf, null, 1);
      server.start();
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to create server");
    } finally {
      server.stop();
    }
  }

  
  private void testPbClientFactory() {
    InetSocketAddress addr = new InetSocketAddress(0);
    System.err.println(addr.getHostName() + addr.getPort());
    Configuration conf = new Configuration();
    ResourceTracker instance = new ResourceTrackerTestImpl();
    Server server = null;
    try {
      server = 
        RpcServerFactoryPBImpl.get().getServer(
            ResourceTracker.class, instance, addr, conf, null, 1);
      server.start();
      System.err.println(server.getListenerAddress());
      System.err.println(NetUtils.getConnectAddress(server));

      ResourceTracker client = null;
      try {
        client = (ResourceTracker) RpcClientFactoryPBImpl.get().getClient(ResourceTracker.class, 1, NetUtils.getConnectAddress(server), conf);
      } catch (YarnException e) {
        e.printStackTrace();
        Assert.fail("Failed to create client");
      }
      
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to create server");
    } finally {
      server.stop();
    }     
  }

  public class ResourceTrackerTestImpl implements ResourceTracker {

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}
