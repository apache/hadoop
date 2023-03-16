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

package org.apache.hadoop.util;

import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains utility methods for dealing with Java Generics. 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GenericsUtil {

  private static final String SLF4J_LOG4J_ADAPTER_CLASS = "org.slf4j.impl.Log4jLoggerAdapter";

  /**
   * Set to false only if log4j adapter class is not found in the classpath. Once set to false,
   * the utility method should not bother re-loading class again.
   */
  private static final AtomicBoolean IS_LOG4J_LOGGER = new AtomicBoolean(true);

  /**
   * Returns the Class object (of type <code>Class&lt;T&gt;</code>) of the  
   * argument of type <code>T</code>. 
   * @param <T> The type of the argument
   * @param t the object to get it class
   * @return <code>Class&lt;T&gt;</code>
   */
  public static <T> Class<T> getClass(T t) {
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>)t.getClass();
    return clazz;
  }

  /**
   * Converts the given <code>List&lt;T&gt;</code> to a an array of 
   * <code>T[]</code>.
   * @param c the Class object of the items in the list
   * @param list the list to convert
   * @param <T> Generics Type T.
   * @return T Array.
   */
  public static <T> T[] toArray(Class<T> c, List<T> list)
  {
    @SuppressWarnings("unchecked")
    T[] ta= (T[])Array.newInstance(c, list.size());

    for (int i= 0; i<list.size(); i++)
      ta[i]= list.get(i);
    return ta;
  }


  /**
   * Converts the given <code>List&lt;T&gt;</code> to a an array of 
   * <code>T[]</code>. 
   * @param list the list to convert
   * @param <T> Generics Type T.
   * @throws ArrayIndexOutOfBoundsException if the list is empty. 
   * Use {@link #toArray(Class, List)} if the list may be empty.
   * @return T Array.
   */
  public static <T> T[] toArray(List<T> list) {
    return toArray(getClass(list.get(0)), list);
  }

  /**
   * Determine whether the log of <code>clazz</code> is Log4j implementation.
   * @param clazz a class to be determined
   * @return true if the log of <code>clazz</code> is Log4j implementation.
   */
  public static boolean isLog4jLogger(Class<?> clazz) {
    if (clazz == null) {
      return false;
    }
    return isLog4jLogger(clazz.getName());
  }

  /**
   * Determine whether the log of the given logger is of Log4J implementation.
   *
   * @param logger the logger name, usually class name as string.
   * @return true if the logger uses Log4J implementation.
   */
  public static boolean isLog4jLogger(String logger) {
    if (logger == null || !IS_LOG4J_LOGGER.get()) {
      return false;
    }
    Logger log = LoggerFactory.getLogger(logger);
    try {
      Class<?> log4jClass = Class.forName(SLF4J_LOG4J_ADAPTER_CLASS);
      return log4jClass.isInstance(log);
    } catch (ClassNotFoundException e) {
      IS_LOG4J_LOGGER.set(false);
      return false;
    }
  }

}
