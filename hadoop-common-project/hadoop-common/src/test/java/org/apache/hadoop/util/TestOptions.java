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

import org.junit.Test;
import static org.junit.Assert.*;

public class TestOptions {

  @Test
  public void testAppend() throws Exception {
    assertArrayEquals("first append",
                      new String[]{"Dr.", "Who", "hi", "there"},
                      Options.prependOptions(new String[]{"hi", "there"},
                                             "Dr.", "Who"));
    assertArrayEquals("second append",
                      new String[]{"aa","bb","cc","dd","ee","ff"},
                      Options.prependOptions(new String[]{"dd", "ee", "ff"},
                                             "aa", "bb", "cc"));
  }

  @Test
  public void testFind() throws Exception {
     Object[] opts = new Object[]{1, "hi", true, "bye", 'x'};
     assertEquals(1, Options.getOption(Integer.class, opts).intValue());
     assertEquals("hi", Options.getOption(String.class, opts));
     assertEquals(true, Options.getOption(Boolean.class, opts).booleanValue());
  }  
}
