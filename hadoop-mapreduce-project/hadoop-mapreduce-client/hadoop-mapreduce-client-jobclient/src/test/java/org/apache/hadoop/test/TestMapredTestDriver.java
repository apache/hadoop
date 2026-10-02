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

package org.apache.hadoop.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ProgramDriver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that every program in {@link MapredTestDriver} can be registered.
 */
public class TestMapredTestDriver {

  /**
   * ProgramDriver#addClass needs a public static main(String[]) on each
   * class, and MapredTestDriver swallows the first failure, which drops
   * every program registered after it, including sleep.
   */
  @Test
  public void testAllProgramsRegister() {
    List<String> registered = new ArrayList<>();
    List<String> failed = new ArrayList<>();
    new MapredTestDriver(new ProgramDriver() {
      @Override
      public void addClass(String name, Class<?> mainClass,
          String description) throws Throwable {
        try {
          super.addClass(name, mainClass, description);
          registered.add(name);
        } catch (Throwable t) {
          failed.add(name + " (" + mainClass.getName() + "): " + t);
          throw t;
        }
      }
    });
    assertEquals(new ArrayList<String>(), failed,
        "Programs that could not be registered");
    assertTrue(registered.contains("sleep"),
        "sleep not registered: " + registered);
  }
}
