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

package org.apache.hadoop.lib.lang;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.test.HTestCase;
import org.junit.Test;

public class TestXException extends HTestCase {

  public static enum TestERROR implements XException.ERROR {
    TC;

    @Override
    public String getTemplate() {
      return "{0}";
    }
  }

  @Test
  public void testXException() throws Exception {
    XException ex = new XException(TestERROR.TC);
    assertEquals(ex.getError(), TestERROR.TC);
    assertEquals(ex.getMessage(), "TC: {0}");
    assertNull(ex.getCause());

    ex = new XException(TestERROR.TC, "msg");
    assertEquals(ex.getError(), TestERROR.TC);
    assertEquals(ex.getMessage(), "TC: msg");
    assertNull(ex.getCause());

    Exception cause = new Exception();
    ex = new XException(TestERROR.TC, cause);
    assertEquals(ex.getError(), TestERROR.TC);
    assertEquals(ex.getMessage(), "TC: " + cause.toString());
    assertEquals(ex.getCause(), cause);

    XException xcause = ex;
    ex = new XException(xcause);
    assertEquals(ex.getError(), TestERROR.TC);
    assertEquals(ex.getMessage(), xcause.getMessage());
    assertEquals(ex.getCause(), xcause);
  }

}
