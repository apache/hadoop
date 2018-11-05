/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import java.lang.reflect.Field;


/**
 * This class includes some functions copied from Mockito's
 * Whitebox class for portability reasons.
 *
 * Whitebox methods are accessed differently in different
 * versions of Hadoop. Specifically the availability of the class
 * changed from Apache Hadoop 3.1.0 to Hadoop 3.2.0.
 *
 * Duplicating the test code is ugly but it allows building
 * HDDS portably.
 */
public final class HddsWhiteboxTestUtils {

  /**
   * Private constructor to disallow construction.
   */
  private HddsWhiteboxTestUtils() {
  }

  /**
   * Get the field of the target object.
   * @param target target object
   * @param field field name
   * @return the field of the object
   */
  public static Object getInternalState(Object target, String field) {
    Class<?> c = target.getClass();
    try {
      Field f = getFieldFromHierarchy(c, field);
      f.setAccessible(true);
      return f.get(target);
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to set internal state on a private field.", e);
    }
  }

  /**
   * Set the field of the target object.
   * @param target target object
   * @param field field name
   * @param value value to set
   */
  public static void setInternalState(
      Object target, String field, Object value) {
    Class<?> c = target.getClass();
    try {
      Field f = getFieldFromHierarchy(c, field);
      f.setAccessible(true);
      f.set(target, value);
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to set internal state on a private field.", e);
    }
  }

  private static Field getFieldFromHierarchy(Class<?> clazz, String field) {
    Field f = getField(clazz, field);
    while (f == null && clazz != Object.class) {
      clazz = clazz.getSuperclass();
      f = getField(clazz, field);
    }
    if (f == null) {
      throw new RuntimeException(
          "You want me to set value to this field: '" + field +
              "' on this class: '" + clazz.getSimpleName() +
              "' but this field is not declared within hierarchy " +
              "of this class!");
    }
    return f;
  }

  private static Field getField(Class<?> clazz, String field) {
    try {
      return clazz.getDeclaredField(field);
    } catch (NoSuchFieldException e) {
      return null;
    }
  }
}
