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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * The class to test DiskValidatorFactory.
 */
public class TestDiskValidatorFactory {

  /**
   * Trivial tests that make sure
   * {@link DiskValidatorFactory#getInstance(String)} works as expected.
   *
   * @throws DiskErrorException if fail to get the instance.
   */
  @Test
  public void testGetInstance() throws DiskErrorException {
    DiskValidator diskValidator = DiskValidatorFactory.getInstance("basic");
    assertNotNull(diskValidator, "Fail to get the instance.");

    assertEquals(diskValidator.getClass(), BasicDiskValidator.class,
        "Fail to create the correct instance.");

    assertNotNull(DiskValidatorFactory.INSTANCES.
        get(BasicDiskValidator.class), "Fail to cache the object");
  }

  /**
   * To test whether an exception is threw out as expected if trying to create
   * a non-exist class.
   * @throws DiskErrorException if fail to get the instance.
   */
  @Test
  public void testGetInstanceOfNonExistClass() throws DiskErrorException {
    assertThrows(DiskErrorException.class, () -> {
      DiskValidatorFactory.getInstance("non-exist");
    });
  }
}
