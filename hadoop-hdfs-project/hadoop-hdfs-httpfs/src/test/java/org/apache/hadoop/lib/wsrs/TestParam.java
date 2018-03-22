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

package org.apache.hadoop.lib.wsrs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.regex.Pattern;

import org.junit.Test;

public class TestParam {

  private <T> void test(Param<T> param, String name,
                   String domain, T defaultValue, T validValue,
                   String invalidStrValue, String outOfRangeValue) throws Exception {

    assertEquals(name, param.getName());
    assertEquals(domain, param.getDomain());
    assertEquals(defaultValue, param.value());
    assertEquals(defaultValue, param.parseParam(""));
    assertEquals(defaultValue, param.parseParam(null));
    assertEquals(validValue, param.parseParam(validValue.toString()));
    if (invalidStrValue != null) {
      try {
        param.parseParam(invalidStrValue);
        fail();
      } catch (IllegalArgumentException ex) {
        //NOP
      } catch (Exception ex) {
        fail();
      }
    }
    if (outOfRangeValue != null) {
      try {
        param.parseParam(outOfRangeValue);
        fail();
      } catch (IllegalArgumentException ex) {
        //NOP
      } catch (Exception ex) {
        fail();
      }
    }
   }

  @Test
  public void testBoolean() throws Exception {
    Param<Boolean> param = new BooleanParam("b", false) {
    };
    test(param, "b", "a boolean", false, true, "x", null);
  }

  @Test
  public void testByte() throws Exception {
    Param<Byte> param = new ByteParam("B", (byte) 1) {
    };
    test(param, "B", "a byte", (byte) 1, (byte) 2, "x", "256");
  }

  @Test
  public void testShort() throws Exception {
    Param<Short> param = new ShortParam("S", (short) 1) {
    };
    test(param, "S", "a short", (short) 1, (short) 2, "x",
         "" + ((int)Short.MAX_VALUE + 1));

    param = new ShortParam("S", (short) 1, 8) {
    };

    assertEquals(new Short((short)01777), param.parse("01777"));
  }

  @Test
  public void testInteger() throws Exception {
    Param<Integer> param = new IntegerParam("I", 1) {
    };
    test(param, "I", "an integer", 1, 2, "x", "" + ((long)Integer.MAX_VALUE + 1));
  }

  @Test
  public void testLong() throws Exception {
    Param<Long> param = new LongParam("L", 1L) {
    };
    test(param, "L", "a long", 1L, 2L, "x", null);
  }

  public enum ENUM {
    FOO, BAR
  }

  @Test
  public void testEnum() throws Exception {
    EnumParam<ENUM> param = new EnumParam<ENUM>("e", ENUM.class, ENUM.FOO) {
    };
    test(param, "e", "FOO,BAR", ENUM.FOO, ENUM.BAR, "x", null);
  }

  @Test
  public void testString() throws Exception {
    Param<String> param = new StringParam("s", "foo") {
    };
    test(param, "s", "a string", "foo", "bar", null, null);
  }

  @Test
  public void testRegEx() throws Exception {
    Param<String> param = new StringParam("r", "aa", Pattern.compile("..")) {
    };
    test(param, "r", "..", "aa", "bb", "c", null);
  }
}
