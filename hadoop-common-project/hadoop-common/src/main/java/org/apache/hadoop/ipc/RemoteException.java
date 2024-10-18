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
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.hadoop.fs.protocolPB.PBHelper;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.ExceptionReconstructProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.xml.sax.Attributes;

public class RemoteException extends IOException {
  /** this value should not be defined in RpcHeader.proto so that protobuf will return a null */
  private static final int UNSPECIFIED_ERROR = -1;
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;
  private final int errorCode;

  private final String className;
  private final ExceptionReconstructProto exceptionReconstructProto;
  
  /**
   * @param className wrapped exception, may be null.
   * @param msg may be null.
   */
  public RemoteException(String className, String msg) {
    this(className, msg, null);
  }
  
  /**
   * @param className wrapped exception, may be null.
   * @param msg may be null.
   * @param erCode may be null.
   */
  public RemoteException(String className, String msg, RpcErrorCodeProto erCode) {
    this(className, msg, erCode, null);
  }

  /**
   * @param className wrapped exception, may be null.
   * @param msg may be null.
   * @param erCode may be null.
   * @param reconstructProto may be null.
   */
  public RemoteException(String className, String msg, RpcErrorCodeProto erCode,
      ExceptionReconstructProto reconstructProto) {
    super(msg);
    this.className = className;
    if (erCode != null) {
      errorCode = erCode.getNumber();
    } else {
      errorCode = UNSPECIFIED_ERROR;
    }
    this.exceptionReconstructProto = reconstructProto;
  }
  
  /**
   * @return the class name for the wrapped exception; may be null if none was given.
   */
  public String getClassName() {
    return className;
  }
  
  /**
   * @return may be null if the code was newer than our protobuf definitions or none was given.
   */
  public RpcErrorCodeProto getErrorCode() {
    return RpcErrorCodeProto.forNumber(errorCode);
  }

  /**
   * If this remote exception wraps up one of the lookupTypes
   * then return this exception.
   * <p>
   * Unwraps any IOException.
   * 
   * @param lookupTypes the desired exception class. may be null.
   * @return IOException, which is either the lookupClass exception or this.
   */
  public IOException unwrapRemoteException(Class<?>... lookupTypes) {
    if(lookupTypes == null)
      return this;
    for(Class<?> lookupClass : lookupTypes) {
      if(!lookupClass.getName().equals(getClassName()))
        continue;
      try {
        return instantiateException(lookupClass.asSubclass(IOException.class));
      } catch(Exception e) {
        // cannot instantiate lookupClass, just return this
        return this;
      }
    }
    // wrapped up exception is not in lookupTypes, just return this
    return this;
  }

  /**
   * Instantiate and return the exception wrapped up by this remote exception.
   * 
   * <p> This unwraps any <code>Throwable</code> that has a constructor taking
   * a <code>String</code> as a parameter.
   * Otherwise it returns this.
   * 
   * @return <code>Throwable</code>
   */
  public IOException unwrapRemoteException() {
    try {
      Class<?> realClass = Class.forName(getClassName());
      return instantiateException(realClass.asSubclass(IOException.class));
    } catch(Exception e) {
      // cannot instantiate the original exception, just return this
    }
    return this;
  }

  private IOException instantiateException(Class<? extends IOException> cls)
      throws Exception {
    Constructor<? extends IOException> cn = cls.getConstructor(String.class);
    cn.setAccessible(true);
    IOException ex = cn.newInstance(this.getMessage());
    if (exceptionReconstructProto != null) {
      // process params
      List<String> params = exceptionReconstructProto.getParamList();
      if (params.size() != 0 && ex instanceof ReconstructibleException) {
        IOException reconstructed = ((ReconstructibleException<?>) ex).
            reconstruct(params.toArray(new String[0]));
        if (reconstructed != null) {
          ex = reconstructed;
        }
      }
      // process stacktrace
      if (exceptionReconstructProto.hasStacktrace()) {
        StackTraceElement[] stacktrace =
            (StackTraceElement[]) PBHelper.toObject(exceptionReconstructProto.getStacktrace());
        ex.setStackTrace(stacktrace);
      }
    }
    ex.initCause(this);
    return ex;
  }

  /**
   * Create RemoteException from attributes.
   * @param attrs may not be null.
   * @return RemoteException.
   */
  public static RemoteException valueOf(Attributes attrs) {
    return new RemoteException(attrs.getValue("class"),
        attrs.getValue("message")); 
  }

  @Override
  public String toString() {
    return getClass().getName() + "(" + className + "): " + getMessage();
  }
}
