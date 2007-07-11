/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
  private RemoteExceptionHandler(){}                    // not instantiable
  
  /**
   * Converts org.apache.hadoop.ipc.RemoteException into original exception,
   * if possible.
   * 
   * @param e original exception
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static void handleRemoteException(final Exception e) throws IOException {
    Exception ex = e;
    if (e instanceof RemoteException) {
      RemoteException r = (RemoteException) e;

      Class c = null;
      try {
        c = Class.forName(r.getClassName());
      
      } catch (ClassNotFoundException x) {
        throw r;
      }
      
      Constructor ctor = null;
      try {
        Class[] parameterTypes = { String.class };
        ctor = c.getConstructor(parameterTypes);
        
      } catch (NoSuchMethodException x) {
        throw r;
      }
      
      try {
        Object[] arguments = { r.getMessage() };

        ex = (Exception) ctor.newInstance(arguments);
        
      } catch (IllegalAccessException x) {
        throw r;
      
      } catch (InvocationTargetException x) {
        throw r;
        
      } catch (InstantiationException x) {
        throw r;
      }
    } 

    if (ex instanceof IOException) {
      IOException io = (IOException) ex;
      throw io;
        
    } else if (ex instanceof RuntimeException) {
      RuntimeException re = (RuntimeException) ex;
      throw re;
        
    } else {
      AssertionError a = new AssertionError("unexpected exception");
      a.initCause(ex);
      throw a;
    }
  }
}
