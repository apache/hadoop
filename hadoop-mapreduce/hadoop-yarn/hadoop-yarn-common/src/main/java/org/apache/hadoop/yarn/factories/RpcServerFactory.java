package org.apache.hadoop.yarn.factories;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;

public interface RpcServerFactory {
  
  public Server getServer(Class<?> protocol, Object instance,
      InetSocketAddress addr, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager,
      int numHandlers)
      throws YarnException;
}