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

package org.apache.hadoop.fs.azurebfs.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Test Mock Helpers.
 */
public final class TestMockHelpers {

  /**
   * Sets a class field by reflection.
   * @param type
   * @param obj
   * @param fieldName
   * @param fieldObject
   * @param <T>
   * @return
   * @throws Exception
   */
  public static <T> T setClassField(
      Class<T> type,
      final T obj,
      final String fieldName,
      Object fieldObject) throws Exception {

    Field field = type.getDeclaredField(fieldName);
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field,
        field.getModifiers() & ~Modifier.FINAL);
    field.set(obj, fieldObject);

    return obj;
  }

  private TestMockHelpers() {
    // Not called. - For checkstyle: HideUtilityClassConstructor
  }
}
