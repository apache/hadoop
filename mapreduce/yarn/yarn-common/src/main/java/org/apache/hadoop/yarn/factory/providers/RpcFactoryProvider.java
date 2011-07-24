package org.apache.hadoop.yarn.factory.providers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;
import org.apache.hadoop.yarn.factories.RpcServerFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;

/**
 * A public static get() method must be present in the Client/Server Factory implementation.
 */
public class RpcFactoryProvider {
  
  //TODO Move these keys to CommonConfigurationKeys
  public static String RPC_SERIALIZER_KEY = "org.apache.yarn.ipc.rpc.serializer.property";
  public static String RPC_SERIALIZER_DEFAULT = "protocolbuffers";

  public static String RPC_CLIENT_FACTORY_CLASS_KEY = "org.apache.yarn.ipc.client.factory.class";
  public static String RPC_SERVER_FACTORY_CLASS_KEY = "org.apache.yarn.ipc.server.factory.class";
  
  private RpcFactoryProvider() {
    
  }
  
  
  public static RpcServerFactory getServerFactory(Configuration conf) {
    if (conf == null) {
      conf = new Configuration();
    }
    String serverFactoryClassName = conf.get(RPC_SERVER_FACTORY_CLASS_KEY);
    if (serverFactoryClassName == null) {
      if (conf.get(RPC_SERIALIZER_KEY, RPC_SERIALIZER_DEFAULT).equals(RPC_SERIALIZER_DEFAULT)) {
        return RpcServerFactoryPBImpl.get();
      } else {
        throw new YarnException("Unknown serializer: [" + conf.get(RPC_SERIALIZER_KEY) + "]. Use keys: [" + RPC_CLIENT_FACTORY_CLASS_KEY + "][" + RPC_SERVER_FACTORY_CLASS_KEY + "] to specify factories");
      }
    } else {
      return (RpcServerFactory) getFactoryClassInstance(serverFactoryClassName);
    }
  }
  
  public static RpcClientFactory getClientFactory(Configuration conf) {
    String clientFactoryClassName = conf.get(RPC_CLIENT_FACTORY_CLASS_KEY);
    if (clientFactoryClassName == null) {
      if (conf.get(RPC_SERIALIZER_KEY, RPC_SERIALIZER_DEFAULT).equals(RPC_SERIALIZER_DEFAULT)) {
        return RpcClientFactoryPBImpl.get();
      } else {
        throw new YarnException("Unknown serializer: [" + conf.get(RPC_SERIALIZER_KEY) + "]. Use keys: [" + RPC_CLIENT_FACTORY_CLASS_KEY + "][" + RPC_SERVER_FACTORY_CLASS_KEY + "] to specify factories");
      }
    } else {
      return(RpcClientFactory) getFactoryClassInstance(clientFactoryClassName);
    }
  }

  private static Object getFactoryClassInstance(String factoryClassName) {
    try {
      Class clazz = Class.forName(factoryClassName);
      Method method = clazz.getMethod("get", null);
      method.setAccessible(true);
      return method.invoke(null, null);
    } catch (ClassNotFoundException e) {
      throw new YarnException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnException(e);
    } catch (InvocationTargetException e) {
      throw new YarnException(e);
    } catch (IllegalAccessException e) {
      throw new YarnException(e);
    }
  }
  
}
