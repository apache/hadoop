package org.apache.hadoop.yarn.factories;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;

public interface RpcClientFactory {
  
  public Object getClient(Class<?> protocol, long clientVersion, InetSocketAddress addr, Configuration conf) throws YarnException;

}