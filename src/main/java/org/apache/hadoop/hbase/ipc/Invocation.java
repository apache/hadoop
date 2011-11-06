/*
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.VersionedWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/** A method invocation, including the method name and its parameters.*/
public class Invocation extends VersionedWritable implements Configurable {
  protected String methodName;
  @SuppressWarnings("rawtypes")
  protected Class[] parameterClasses;
  protected Object[] parameters;
  protected Configuration conf;
  private long clientVersion;
  private int clientMethodsHash;

  private static byte RPC_VERSION = 1;

  public Invocation() {}

  public Invocation(Method method, Object[] parameters) {
    this.methodName = method.getName();
    this.parameterClasses = method.getParameterTypes();
    this.parameters = parameters;
    if (method.getDeclaringClass().equals(VersionedProtocol.class)) {
      //VersionedProtocol is exempted from version check.
      clientVersion = 0;
      clientMethodsHash = 0;
    } else {
      try {
        Field versionField = method.getDeclaringClass().getField("VERSION");
        versionField.setAccessible(true);
        this.clientVersion = versionField.getLong(method.getDeclaringClass());
      } catch (NoSuchFieldException ex) {
        throw new RuntimeException("The " + method.getDeclaringClass(), ex);
      } catch (IllegalAccessException ex) {
        throw new RuntimeException(ex);
      }
      this.clientMethodsHash = ProtocolSignature.getFingerprint(method
          .getDeclaringClass().getMethods());
    }
  }

  /** @return The name of the method invoked. */
  public String getMethodName() { return methodName; }

  /** @return The parameter classes. */
  @SuppressWarnings({ "rawtypes" })
  public Class[] getParameterClasses() { return parameterClasses; }

  /** @return The parameter instances. */
  public Object[] getParameters() { return parameters; }

  long getProtocolVersion() {
    return clientVersion;
  }

  protected int getClientMethodsHash() {
    return clientMethodsHash;
  }

  /**
   * Returns the rpc version used by the client.
   * @return rpcVersion
   */
  public long getRpcVersion() {
    return RPC_VERSION;
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    methodName = in.readUTF();
    clientVersion = in.readLong();
    clientMethodsHash = in.readInt();
    parameters = new Object[in.readInt()];
    parameterClasses = new Class[parameters.length];
    HbaseObjectWritable objectWritable = new HbaseObjectWritable();
    for (int i = 0; i < parameters.length; i++) {
      parameters[i] = HbaseObjectWritable.readObject(in, objectWritable,
        this.conf);
      parameterClasses[i] = objectWritable.getDeclaredClass();
    }
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(this.methodName);
    out.writeLong(clientVersion);
    out.writeInt(clientMethodsHash);
    out.writeInt(parameterClasses.length);
    for (int i = 0; i < parameterClasses.length; i++) {
      HbaseObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                 conf);
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder(256);
    buffer.append(methodName);
    buffer.append("(");
    for (int i = 0; i < parameters.length; i++) {
      if (i != 0)
        buffer.append(", ");
      buffer.append(parameters[i]);
    }
    buffer.append(")");
    buffer.append(", rpc version="+RPC_VERSION);
    buffer.append(", client version="+clientVersion);
    buffer.append(", methodsFingerPrint="+clientMethodsHash);
    return buffer.toString();
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public byte getVersion() {
    return RPC_VERSION;
  }
}