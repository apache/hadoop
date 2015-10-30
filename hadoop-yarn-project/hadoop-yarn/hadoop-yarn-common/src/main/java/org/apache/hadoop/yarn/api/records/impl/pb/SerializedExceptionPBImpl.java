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

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProtoOrBuilder;

public class SerializedExceptionPBImpl extends SerializedException {

  SerializedExceptionProto proto = null;
  SerializedExceptionProto.Builder builder =
      SerializedExceptionProto.newBuilder();
  boolean viaProto = false;

  public SerializedExceptionPBImpl() {
  }

  public SerializedExceptionPBImpl(SerializedExceptionProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private SerializedExceptionPBImpl(Throwable t) {
    init(t);
  }

  public void init(String message) {
    maybeInitBuilder();
    builder.setMessage(message);
  }

  public void init(Throwable t) {
    maybeInitBuilder();
    if (t == null) {
      return;
    }

    if (t.getCause() == null) {
    } else {
      builder.setCause(new SerializedExceptionPBImpl(t.getCause()).getProto());
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    pw.close();
    if (sw.toString() != null)
      builder.setTrace(sw.toString());
    if (t.getMessage() != null)
      builder.setMessage(t.getMessage());
    builder.setClassName(t.getClass().getCanonicalName());
  }

  public void init(String message, Throwable t) {
    init(t);
    if (message != null)
      builder.setMessage(message);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Throwable deSerialize() {

    SerializedException cause = getCause();
    SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
    Class<?> realClass = null;
    try {
      realClass = Class.forName(p.getClassName());
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(e);
    }
    Class classType = null;
    if (YarnException.class.isAssignableFrom(realClass)) {
      classType = YarnException.class;
    } else if (IOException.class.isAssignableFrom(realClass)) {
      classType = IOException.class;
    } else if (RuntimeException.class.isAssignableFrom(realClass)) {
      classType = RuntimeException.class;
    } else {
      classType = Throwable.class;
    }
    return instantiateException(realClass.asSubclass(classType), getMessage(),
      cause == null ? null : cause.deSerialize());
  }

  @Override
  public String getMessage() {
    SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMessage();
  }

  @Override
  public String getRemoteTrace() {
    SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTrace();
  }

  @Override
  public SerializedException getCause() {
    SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasCause()) {
      return new SerializedExceptionPBImpl(p.getCause());
    } else {
      return null;
    }
  }

  public SerializedExceptionProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SerializedExceptionProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private static <T extends Throwable> T instantiateExceptionImpl(
      String message, Class<? extends T> cls, Throwable cause)
      throws NoSuchMethodException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    Constructor<? extends T> cn;
    T ex = null;
    cn =
        cls.getConstructor(message == null ? new Class[0]
            : new Class[] {String.class});
    cn.setAccessible(true);
    ex = message == null ? cn.newInstance() : cn.newInstance(message);
    ex.initCause(cause);
    return ex;
  }

  private static <T extends Throwable> T instantiateException(
      Class<? extends T> cls, String message, Throwable cause) {
    T ex = null;
    try {
      // Try constructor with String argument, if it fails, try default.
      try {
        ex = instantiateExceptionImpl(message, cls, cause);
      } catch (NoSuchMethodException e) {
        ex = instantiateExceptionImpl(null, cls, cause);
      }
    } catch (SecurityException e) {
      throw new YarnRuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new YarnRuntimeException(e);
    } catch (InstantiationException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    }
    return ex;
  }
}