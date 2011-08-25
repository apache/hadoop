package org.apache.hadoop.yarn;

import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
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
    AMRMProtocol instance = new AMRMProtocolTestImpl();
    Server server = null;
    try {
      server = 
        RpcServerFactoryPBImpl.get().getServer(
            AMRMProtocol.class, instance, addr, conf, null, 1);
      server.start();
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to create server");
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  
  private void testPbClientFactory() {
    InetSocketAddress addr = new InetSocketAddress(0);
    System.err.println(addr.getHostName() + addr.getPort());
    Configuration conf = new Configuration();
    AMRMProtocol instance = new AMRMProtocolTestImpl();
    Server server = null;
    try {
      server = 
        RpcServerFactoryPBImpl.get().getServer(
            AMRMProtocol.class, instance, addr, conf, null, 1);
      server.start();
      System.err.println(server.getListenerAddress());
      System.err.println(NetUtils.getConnectAddress(server));

      AMRMProtocol amrmClient = null;
      try {
        amrmClient = (AMRMProtocol) RpcClientFactoryPBImpl.get().getClient(AMRMProtocol.class, 1, NetUtils.getConnectAddress(server), conf);
      } catch (YarnException e) {
        e.printStackTrace();
        Assert.fail("Failed to create client");
      }
      
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to create server");
    } finally {
      if (server != null) {
        server.stop();
      }
    }     
  }

  public class AMRMProtocolTestImpl implements AMRMProtocol {

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request) throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FinishApplicationMasterResponse finishApplicationMaster(
        FinishApplicationMasterRequest request) throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public AllocateResponse allocate(AllocateRequest request)
        throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
