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

package org.apache.hadoop.yarn.ipc;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
import org.apache.hadoop.yarn.factories.YarnRemoteExceptionFactory;
import org.apache.hadoop.yarn.factory.providers.YarnRemoteExceptionFactoryProvider;

import com.google.protobuf.ServiceException;

public class RPCUtil {


  /**
   * Relying on the default factory configuration to be set correctly
   * for the default configuration.
   */
  private static Configuration conf = new Configuration();
  private static YarnRemoteExceptionFactory exceptionFactory = YarnRemoteExceptionFactoryProvider.getYarnRemoteExceptionFactory(conf);
  
  /**
   * Returns the YarnRemoteException which is serializable. 
   */
  public static YarnRemoteException getRemoteException(Throwable t) {
    return exceptionFactory.createYarnRemoteException(t);
  }

  /**
   * Returns the YarnRemoteException which is serializable.
   */
  public static YarnRemoteException getRemoteException(String message) {
    return exceptionFactory.createYarnRemoteException(message);
  }

  public static String toString(YarnRemoteException e) {
    return (e.getMessage() == null ? "" : e.getMessage()) + 
      (e.getRemoteTrace() == null ? "" : "\n StackTrace: " + e.getRemoteTrace()) + 
      (e.getCause() == null ? "" : "\n Caused by: " + toString(e.getCause()));
  }
  
  /**
   * Utility method that unwraps and throws appropriate exception.
   * 
   * @param se ServiceException
   * @throws YarnRemoteException
   * @throws UndeclaredThrowableException
   */
  public static YarnRemoteException unwrapAndThrowException(ServiceException se)
      throws UndeclaredThrowableException {
    if (se.getCause() instanceof RemoteException) {
      try {
        RemoteException re = (RemoteException) se.getCause();
        Class<?> realClass = Class.forName(re.getClassName());
        //YarnRemoteException is not rooted as IOException.
        //Do the explicitly check if it is YarnRemoteException
        if (YarnRemoteException.class.isAssignableFrom(realClass)) {
          Constructor<? extends YarnRemoteException> cn =
              realClass.asSubclass(YarnRemoteException.class).getConstructor(
                  String.class);
          cn.setAccessible(true);
          YarnRemoteException ex = cn.newInstance(re.getMessage());
          ex.initCause(re);
          return ex;
        } else {
          throw ((RemoteException) se.getCause())
              .unwrapRemoteException(YarnRemoteExceptionPBImpl.class);
        }
      } catch (IOException e1) {
        throw new UndeclaredThrowableException(e1);
      } catch (Exception ex) {
        throw new UndeclaredThrowableException(
            (RemoteException) se.getCause());
      }
    } else if (se.getCause() instanceof YarnRemoteException) {
      return (YarnRemoteException) se.getCause();
    } else if (se.getCause() instanceof UndeclaredThrowableException) {
      throw (UndeclaredThrowableException) se.getCause();
    } else {
      throw new UndeclaredThrowableException(se);
    }
  }
}
