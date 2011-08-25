package org.apache.hadoop.yarn;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.factories.RpcClientFactory;
import org.apache.hadoop.yarn.factories.RpcServerFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
import org.apache.hadoop.yarn.factory.providers.RpcFactoryProvider;
import org.junit.Test;

public class TestRpcFactoryProvider {

  @Test
  public void testFactoryProvider() {
    Configuration conf = new Configuration();
    RpcClientFactory clientFactory = null;
    RpcServerFactory serverFactory = null;
    
    
    clientFactory = RpcFactoryProvider.getClientFactory(conf);
    serverFactory = RpcFactoryProvider.getServerFactory(conf);
    Assert.assertEquals(RpcClientFactoryPBImpl.class, clientFactory.getClass());
    Assert.assertEquals(RpcServerFactoryPBImpl.class, serverFactory.getClass());
    
    conf.set(RpcFactoryProvider.RPC_SERIALIZER_KEY, "writable");
    try {
      clientFactory = RpcFactoryProvider.getClientFactory(conf);
      Assert.fail("Expected an exception - unknown serializer");
    } catch (YarnException e) {
    }
    try {
      serverFactory = RpcFactoryProvider.getServerFactory(conf);
      Assert.fail("Expected an exception - unknown serializer");
    } catch (YarnException e) {
    }
    
    conf = new Configuration();
    conf.set(RpcFactoryProvider.RPC_CLIENT_FACTORY_CLASS_KEY, "NonExistantClass");
    conf.set(RpcFactoryProvider.RPC_SERVER_FACTORY_CLASS_KEY, RpcServerFactoryPBImpl.class.getName());
    
    try {
      clientFactory = RpcFactoryProvider.getClientFactory(conf);
      Assert.fail("Expected an exception - unknown class");
    } catch (YarnException e) {
    }
    try {
      serverFactory = RpcFactoryProvider.getServerFactory(conf);
    } catch (YarnException e) {
      Assert.fail("Error while loading factory using reflection: [" + RpcServerFactoryPBImpl.class.getName() + "]");
    }
  }
}
