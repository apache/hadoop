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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import com.google.common.annotations.VisibleForTesting;

public class ProtocolSignature implements Writable {
  static {               // register a ctor
    WritableFactories.setFactory
      (ProtocolSignature.class,
       new WritableFactory() {
         @Override
        public Writable newInstance() { return new ProtocolSignature(); }
       });
  }

  private long version;
  private int[] methods = null; // an array of method hash codes
  
  /**
   * default constructor
   */
  public ProtocolSignature() {
  }
  
  /**
   * Constructor
   * 
   * @param version server version
   * @param methodHashcodes hash codes of the methods supported by server
   */
  public ProtocolSignature(long version, int[] methodHashcodes) {
    this.version = version;
    this.methods = methodHashcodes;
  }
  
  public long getVersion() {
    return version;
  }
  
  public int[] getMethods() {
    return methods;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    version = in.readLong();
    boolean hasMethods = in.readBoolean();
    if (hasMethods) {
      int numMethods = in.readInt();
      methods = new int[numMethods];
      for (int i=0; i<numMethods; i++) {
        methods[i] = in.readInt();
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(version);
    if (methods == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(methods.length);
      for (int method : methods) {
        out.writeInt(method);
      }
    }
  }

  /**
   * Calculate a method's hash code considering its method
   * name, returning type, and its parameter types
   * 
   * @param method a method
   * @return its hash code
   */
  static int getFingerprint(Method method) {
    int hashcode = method.getName().hashCode();
    hashcode =  hashcode + 31*method.getReturnType().getName().hashCode();
    for (Class<?> type : method.getParameterTypes()) {
      hashcode = 31*hashcode ^ type.getName().hashCode();
    }
    return hashcode;
  }

  /**
   * Convert an array of Method into an array of hash codes
   * 
   * @param methods
   * @return array of hash codes
   */
  private static int[] getFingerprints(Method[] methods) {
    if (methods == null) {
      return null;
    }
    int[] hashCodes = new int[methods.length];
    for (int i = 0; i<methods.length; i++) {
      hashCodes[i] = getFingerprint(methods[i]);
    }
    return hashCodes;
  }

  /**
   * Get the hash code of an array of methods
   * Methods are sorted before hashcode is calculated.
   * So the returned value is irrelevant of the method order in the array.
   * 
   * @param methods an array of methods
   * @return the hash code
   */
  static int getFingerprint(Method[] methods) {
    return getFingerprint(getFingerprints(methods));
  }
  
  /**
   * Get the hash code of an array of hashcodes
   * Hashcodes are sorted before hashcode is calculated.
   * So the returned value is irrelevant of the hashcode order in the array.
   * 
   * @param methods an array of methods
   * @return the hash code
   */
  static int getFingerprint(int[] hashcodes) {
    Arrays.sort(hashcodes);
    return Arrays.hashCode(hashcodes);
    
  }
  private static class ProtocolSigFingerprint {
    private ProtocolSignature signature;
    private int fingerprint;
    
    ProtocolSigFingerprint(ProtocolSignature sig, int fingerprint) {
      this.signature = sig;
      this.fingerprint = fingerprint;
    }
  }
  
  /**
   * A cache that maps a protocol's name to its signature & finger print
   */
  private final static HashMap<String, ProtocolSigFingerprint> 
     PROTOCOL_FINGERPRINT_CACHE = 
       new HashMap<String, ProtocolSigFingerprint>();
  
  @VisibleForTesting
  public static void resetCache() {
    PROTOCOL_FINGERPRINT_CACHE.clear();
  }
  
  /**
   * Return a protocol's signature and finger print from cache
   * 
   * @param protocol a protocol class
   * @param serverVersion protocol version
   * @return its signature and finger print
   */
  private static ProtocolSigFingerprint getSigFingerprint(
      Class <?> protocol, long serverVersion) {
    String protocolName = RPC.getProtocolName(protocol);
    synchronized (PROTOCOL_FINGERPRINT_CACHE) {
      ProtocolSigFingerprint sig = PROTOCOL_FINGERPRINT_CACHE.get(protocolName);
      if (sig == null) {
        int[] serverMethodHashcodes = getFingerprints(protocol.getMethods());
        sig = new ProtocolSigFingerprint(
            new ProtocolSignature(serverVersion, serverMethodHashcodes),
            getFingerprint(serverMethodHashcodes));
        PROTOCOL_FINGERPRINT_CACHE.put(protocolName, sig);
      }
      return sig;    
    }
  }
  
  /**
   * Get a server protocol's signature
   * 
   * @param clientMethodsHashCode client protocol methods hashcode
   * @param serverVersion server protocol version
   * @param protocol protocol
   * @return the server's protocol signature
   */
  public static ProtocolSignature getProtocolSignature(
      int clientMethodsHashCode,
      long serverVersion,
      Class<? extends VersionedProtocol> protocol) {
    // try to get the finger print & signature from the cache
    ProtocolSigFingerprint sig = getSigFingerprint(protocol, serverVersion);
    
    // check if the client side protocol matches the one on the server side
    if (clientMethodsHashCode == sig.fingerprint) {
      return new ProtocolSignature(serverVersion, null);  // null indicates a match
    } 
    
    return sig.signature;
  }
  
  public static ProtocolSignature getProtocolSignature(String protocolName,
      long version) throws ClassNotFoundException {
    Class<?> protocol = Class.forName(protocolName);
    return getSigFingerprint(protocol, version).signature;
  }
  
  /**
   * Get a server protocol's signature
   *
   * @param server server implementation
   * @param protocol server protocol
   * @param clientVersion client's version
   * @param clientMethodsHash client's protocol's hash code
   * @return the server protocol's signature
   * @throws IOException if any error occurs
   */
  @SuppressWarnings("unchecked")
  public static ProtocolSignature getProtocolSignature(VersionedProtocol server,
      String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    Class<? extends VersionedProtocol> inter;
    try {
      inter = (Class<? extends VersionedProtocol>)Class.forName(protocol);
    } catch (Exception e) {
      throw new IOException(e);
    }
    long serverVersion = server.getProtocolVersion(protocol, clientVersion);
    return ProtocolSignature.getProtocolSignature(
        clientMethodsHash, serverVersion, inter);
  }
}
