/*
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Methods {
  private static Log LOG = LogFactory.getLog(Methods.class);

  public static <T> Object call(Class<T> clazz, T instance, String methodName,
      Class[] types, Object[] args) throws Exception {
    try {
      Method m = clazz.getMethod(methodName, types);
      return m.invoke(instance, args);
    } catch (IllegalArgumentException arge) {
      LOG.fatal("Constructed invalid call. class="+clazz.getName()+
          " method=" + methodName + " types=" + Classes.stringify(types), arge);
      throw arge;
    } catch (NoSuchMethodException nsme) {
      throw new IllegalArgumentException(
          "Can't find method "+methodName+" in "+clazz.getName()+"!", nsme);
    } catch (InvocationTargetException ite) {
      // unwrap the underlying exception and rethrow
      if (ite.getTargetException() != null) {
        if (ite.getTargetException() instanceof Exception) {
          throw (Exception)ite.getTargetException();
        } else if (ite.getTargetException() instanceof Error) {
          throw (Error)ite.getTargetException();
        }
      }
      throw new UndeclaredThrowableException(ite,
          "Unknown exception invoking "+clazz.getName()+"."+methodName+"()");
    } catch (IllegalAccessException iae) {
      throw new IllegalArgumentException(
          "Denied access calling "+clazz.getName()+"."+methodName+"()", iae);
    } catch (SecurityException se) {
      LOG.fatal("SecurityException calling method. class="+clazz.getName()+
          " method=" + methodName + " types=" + Classes.stringify(types), se);
      throw se;
    }
  }
}
