/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.ipc.RemoteException;

/**
 * An immutable class which contains a static method for handling
 * org.apache.hadoop.ipc.RemoteException exceptions.
 */
public class RemoteExceptionHandler {
  /* Not instantiable */
  private RemoteExceptionHandler() {super();}

  /**
   * Examine passed Throwable.  See if its carrying a RemoteException. If so,
   * run {@link #decodeRemoteException(RemoteException)} on it.  Otherwise,
   * pass back <code>t</code> unaltered.
   * @param t Throwable to examine.
   * @return Decoded RemoteException carried by <code>t</code> or
   * <code>t</code> unaltered.
   */
  public static Throwable checkThrowable(final Throwable t) {
    Throwable result = t;
    if (t instanceof RemoteException) {
      try {
        result =
          RemoteExceptionHandler.decodeRemoteException((RemoteException)t);
      } catch (Throwable tt) {
        result = tt;
      }
    }
    return result;
  }

  /**
   * Examine passed IOException.  See if its carrying a RemoteException. If so,
   * run {@link #decodeRemoteException(RemoteException)} on it.  Otherwise,
   * pass back <code>e</code> unaltered.
   * @param e Exception to examine.
   * @return Decoded RemoteException carried by <code>e</code> or
   * <code>e</code> unaltered.
   */
  public static IOException checkIOException(final IOException e) {
    Throwable t = checkThrowable(e);
    return t instanceof IOException? (IOException)t: new IOException(t);
  }

  /**
   * Converts org.apache.hadoop.ipc.RemoteException into original exception,
   * if possible. If the original exception is an Error or a RuntimeException,
   * throws the original exception.
   *
   * @param re original exception
   * @return decoded RemoteException if it is an instance of or a subclass of
   *         IOException, or the original RemoteException if it cannot be decoded.
   *
   * @throws IOException indicating a server error ocurred if the decoded
   *         exception is not an IOException. The decoded exception is set as
   *         the cause.
   * @deprecated Use {@link RemoteException#unwrapRemoteException()} instead.
   * In fact we should look into deprecating this whole class - St.Ack 2010929
   */
  public static IOException decodeRemoteException(final RemoteException re)
  throws IOException {
    IOException i = re;

    try {
      Class<?> c = Class.forName(re.getClassName());

      Class<?>[] parameterTypes = { String.class };
      Constructor<?> ctor = c.getConstructor(parameterTypes);

      Object[] arguments = { re.getMessage() };
      Throwable t = (Throwable) ctor.newInstance(arguments);

      if (t instanceof IOException) {
        i = (IOException) t;

      } else {
        i = new IOException("server error");
        i.initCause(t);
        throw i;
      }

    } catch (ClassNotFoundException x) {
      // continue
    } catch (NoSuchMethodException x) {
      // continue
    } catch (IllegalAccessException x) {
      // continue
    } catch (InvocationTargetException x) {
      // continue
    } catch (InstantiationException x) {
      // continue
    }
    return i;
  }
}
