/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto;
import org.apache.hadoop.net.NetUtils;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class maintains a cache of protocol versions and corresponding protocol
 * signatures, keyed by server address, protocol and rpc kind.
 * The cache is lazily populated. 
 */
public class RpcClientUtil {
  private static RpcController NULL_CONTROLLER = null;
  private static final int PRIME = 16777619;
  
  private static class ProtoSigCacheKey {
    private InetSocketAddress serverAddress;
    private String protocol;
    private String rpcKind;
    
    ProtoSigCacheKey(InetSocketAddress addr, String p, String rk) {
      this.serverAddress = addr;
      this.protocol = p;
      this.rpcKind = rk;
    }
    
    @Override //Object
    public int hashCode() {
      int result = 1;
      result = PRIME * result
          + ((serverAddress == null) ? 0 : serverAddress.hashCode());
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + ((rpcKind == null) ? 0 : rpcKind.hashCode());
      return result;
    }
    
    @Override //Object
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (other instanceof ProtoSigCacheKey) {
        ProtoSigCacheKey otherKey = (ProtoSigCacheKey) other;
        return (serverAddress.equals(otherKey.serverAddress) &&
            protocol.equals(otherKey.protocol) &&
            rpcKind.equals(otherKey.rpcKind));
      }
      return false;
    }
  }
  
  private static ConcurrentHashMap<ProtoSigCacheKey, Map<Long, ProtocolSignature>> 
  signatureMap = new ConcurrentHashMap<ProtoSigCacheKey, Map<Long, ProtocolSignature>>();

  private static void putVersionSignatureMap(InetSocketAddress addr,
      String protocol, String rpcKind, Map<Long, ProtocolSignature> map) {
    signatureMap.put(new ProtoSigCacheKey(addr, protocol, rpcKind), map);
  }
  
  private static Map<Long, ProtocolSignature> getVersionSignatureMap(
      InetSocketAddress addr, String protocol, String rpcKind) {
    return signatureMap.get(new ProtoSigCacheKey(addr, protocol, rpcKind));
  }

  /**
   * Returns whether the given method is supported or not.
   * The protocol signatures are fetched and cached. The connection id for the
   * proxy provided is re-used.
   * @param rpcProxy Proxy which provides an existing connection id.
   * @param protocol Protocol for which the method check is required.
   * @param rpcKind The RpcKind for which the method check is required.
   * @param version The version at the client.
   * @param methodName Name of the method.
   * @return true if the method is supported, false otherwise.
   * @throws IOException
   */
  public static boolean isMethodSupported(Object rpcProxy, Class<?> protocol,
      RPC.RpcKind rpcKind, long version, String methodName) throws IOException {
    InetSocketAddress serverAddress = RPC.getServerAddress(rpcProxy);
    Map<Long, ProtocolSignature> versionMap = getVersionSignatureMap(
        serverAddress, protocol.getName(), rpcKind.toString());

    if (versionMap == null) {
      Configuration conf = new Configuration();
      RPC.setProtocolEngine(conf, ProtocolMetaInfoPB.class,
          ProtobufRpcEngine.class);
      ProtocolMetaInfoPB protocolInfoProxy = getProtocolMetaInfoProxy(rpcProxy,
          conf);
      GetProtocolSignatureRequestProto.Builder builder = 
          GetProtocolSignatureRequestProto.newBuilder();
      builder.setProtocol(protocol.getName());
      builder.setRpcKind(rpcKind.toString());
      GetProtocolSignatureResponseProto resp;
      try {
        resp = protocolInfoProxy.getProtocolSignature(NULL_CONTROLLER,
            builder.build());
      } catch (ServiceException se) {
        throw ProtobufHelper.getRemoteException(se);
      }
      versionMap = convertProtocolSignatureProtos(resp
          .getProtocolSignatureList());
      putVersionSignatureMap(serverAddress, protocol.getName(),
          rpcKind.toString(), versionMap);
    }
    // Assuming unique method names.
    Method desiredMethod;
    Method[] allMethods = protocol.getMethods();
    desiredMethod = null;
    for (Method m : allMethods) {
      if (m.getName().equals(methodName)) {
        desiredMethod = m;
        break;
      }
    }
    if (desiredMethod == null) {
      return false;
    }
    int methodHash = ProtocolSignature.getFingerprint(desiredMethod);
    return methodExists(methodHash, version, versionMap);
  }
  
  private static Map<Long, ProtocolSignature> 
  convertProtocolSignatureProtos(List<ProtocolSignatureProto> protoList) {
    Map<Long, ProtocolSignature> map = new TreeMap<Long, ProtocolSignature>();
    for (ProtocolSignatureProto p : protoList) {
      int [] methods = new int[p.getMethodsList().size()];
      int index=0;
      for (int m : p.getMethodsList()) {
        methods[index++] = m;
      }
      map.put(p.getVersion(), new ProtocolSignature(p.getVersion(), methods));
    }
    return map;
  }

  private static boolean methodExists(int methodHash, long version,
      Map<Long, ProtocolSignature> versionMap) {
    ProtocolSignature sig = versionMap.get(version);
    if (sig != null) {
      for (int m : sig.getMethods()) {
        if (m == methodHash) {
          return true;
        }
      }
    }
    return false;
  }
  
  // The proxy returned re-uses the underlying connection. This is a special 
  // mechanism for ProtocolMetaInfoPB.
  // Don't do this for any other protocol, it might cause a security hole.
  private static ProtocolMetaInfoPB getProtocolMetaInfoProxy(Object proxy,
      Configuration conf) throws IOException {
    RpcInvocationHandler inv = (RpcInvocationHandler) Proxy
        .getInvocationHandler(proxy);
    return RPC
        .getProtocolEngine(ProtocolMetaInfoPB.class, conf)
        .getProtocolMetaInfoProxy(inv.getConnectionId(), conf,
            NetUtils.getDefaultSocketFactory(conf)).getProxy();
  }

  /**
   * Convert an RPC method to a string.
   * The format we want is 'MethodOuterClassShortName#methodName'.
   *
   * For example, if the method is:
   *   org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.
   *     ClientNamenodeProtocol.BlockingInterface.getServerDefaults
   *
   * the format we want is:
   *   ClientNamenodeProtocol#getServerDefaults
   */
  public static String methodToTraceString(Method method) {
    Class<?> clazz = method.getDeclaringClass();
    while (true) {
      Class<?> next = clazz.getEnclosingClass();
      if (next == null || next.getEnclosingClass() == null) break;
      clazz = next;
    }
    return clazz.getSimpleName() + "#" + method.getName();
  }
}
