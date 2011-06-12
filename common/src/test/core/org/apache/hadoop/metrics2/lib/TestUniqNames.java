/*
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

package org.apache.hadoop.metrics2.lib;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestUniqNames {

  @Test public void testCommonCases() {
    UniqueNames u = new UniqueNames();

    assertEquals("foo", u.uniqueName("foo"));
    assertEquals("foo-1", u.uniqueName("foo"));
  }

  @Test public void testCollisions() {
    UniqueNames u = new UniqueNames();
    u.uniqueName("foo");

    assertEquals("foo-1", u.uniqueName("foo-1"));
    assertEquals("foo-2", u.uniqueName("foo"));
    assertEquals("foo-1-1", u.uniqueName("foo-1"));
    assertEquals("foo-2-1", u.uniqueName("foo-2"));
  }
}
