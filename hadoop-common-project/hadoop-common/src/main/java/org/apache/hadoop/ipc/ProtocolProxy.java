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
import java.util.HashSet;


/**
 * a class wraps around a server's proxy, 
 * containing a list of its supported methods.
 * 
 * A list of methods with a value of null indicates that the client and server
 * have the same protocol.
 */
public class ProtocolProxy<T> {
  private Class<T> protocol;
  private T proxy;
  private HashSet<Integer> serverMethods = null;
  final private boolean supportServerMethodCheck;
  private boolean serverMethodsFetched = false;
  
  /**
   * Constructor
   * 
   * @param protocol protocol class
   * @param proxy its proxy
   * @param supportServerMethodCheck If false proxy will never fetch server
   *        methods and isMethodSupported will always return true. If true,
   *        server methods will be fetched for the first call to 
   *        isMethodSupported. 
   */
  public ProtocolProxy(Class<T> protocol, T proxy,
      boolean supportServerMethodCheck) {
    this.protocol = protocol;
    this.proxy = proxy;
    this.supportServerMethodCheck = supportServerMethodCheck;
  }
  
  private void fetchServerMethods(Method method) throws IOException {
    long clientVersion;
    clientVersion = RPC.getProtocolVersion(method.getDeclaringClass());
    int clientMethodsHash = ProtocolSignature.getFingerprint(method
        .getDeclaringClass().getMethods());
    ProtocolSignature serverInfo = ((VersionedProtocol) proxy)
        .getProtocolSignature(RPC.getProtocolName(protocol), clientVersion,
            clientMethodsHash);
    long serverVersion = serverInfo.getVersion();
    if (serverVersion != clientVersion) {
      throw new RPC.VersionMismatch(protocol.getName(), clientVersion,
          serverVersion);
    }
    int[] serverMethodsCodes = serverInfo.getMethods();
    if (serverMethodsCodes != null) {
      serverMethods = new HashSet<Integer>(serverMethodsCodes.length);
      for (int m : serverMethodsCodes) {
        this.serverMethods.add(Integer.valueOf(m));
      }
    }
    serverMethodsFetched = true;
  }

  /*
   * Get the proxy
   */
  public T getProxy() {
    return proxy;
  }
  
  /**
   * Check if a method is supported by the server or not
   * 
   * @param methodName a method's name in String format
   * @param parameterTypes a method's parameter types
   * @return true if the method is supported by the server
   */
  public synchronized boolean isMethodSupported(String methodName,
                                   Class<?>... parameterTypes)
  throws IOException {
    if (!supportServerMethodCheck) {
      return true;
    }
    Method method;
    try {
      method = protocol.getDeclaredMethod(methodName, parameterTypes);
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (NoSuchMethodException e) {
      throw new IOException(e);
    }
    if (!serverMethodsFetched) {
      fetchServerMethods(method);
    }
    if (serverMethods == null) { // client & server have the same protocol
      return true;
    }
    return serverMethods.contains(
        Integer.valueOf(ProtocolSignature.getFingerprint(method)));
  }
}