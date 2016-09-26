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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;

/**
 * Test the test utils. Why an integration test? it's needed to
 * verify property pushdown.
 */
public class ITestS3ATestUtils extends Assert {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ATestUtils.class);
  public static final String KEY = "undefined.property";

  @Before
  public void clear() {
    System.clearProperty(KEY);
  }

  @Test
  public void testGetTestProperty() throws Throwable {
    Configuration conf = new Configuration(false);
    assertEquals("a", getTestProperty(conf, KEY, "a"));
    conf.set(KEY, "\t b \n");
    assertEquals("b", getTestProperty(conf, KEY, "a"));
    System.setProperty(KEY, "c");
    assertEquals("c", getTestProperty(conf, KEY, "a"));
    unsetSysprop();
    assertEquals("b", getTestProperty(conf, KEY, "a"));
  }

  @Test
  public void testGetTestPropertyLong() throws Throwable {
    Configuration conf = new Configuration(false);
    assertEquals(1, getTestPropertyLong(conf, KEY, 1));
    conf.setInt(KEY, 2);
    assertEquals(2, getTestPropertyLong(conf, KEY, 1));
    System.setProperty(KEY, "3");
    assertEquals(3, getTestPropertyLong(conf, KEY, 1));
  }

  @Test
  public void testGetTestPropertyInt() throws Throwable {
    Configuration conf = new Configuration(false);
    assertEquals(1, getTestPropertyInt(conf, KEY, 1));
    conf.setInt(KEY, 2);
    assertEquals(2, getTestPropertyInt(conf, KEY, 1));
    System.setProperty(KEY, "3");
    assertEquals(3, getTestPropertyInt(conf, KEY, 1));
    conf.unset(KEY);
    assertEquals(3, getTestPropertyInt(conf, KEY, 1));
    unsetSysprop();
    assertEquals(5, getTestPropertyInt(conf, KEY, 5));
  }

  @Test
  public void testGetTestPropertyBool() throws Throwable {
    Configuration conf = new Configuration(false);
    assertTrue(getTestPropertyBool(conf, KEY, true));
    conf.set(KEY, "\tfalse \n");
    assertFalse(getTestPropertyBool(conf, KEY, true));
    System.setProperty(KEY, "true");
    assertTrue(getTestPropertyBool(conf, KEY, true));
    unsetSysprop();
    assertEquals("false", getTestProperty(conf, KEY, "true"));
    conf.unset(KEY);
    assertTrue(getTestPropertyBool(conf, KEY, true));
  }

  protected void unsetSysprop() {
    System.setProperty(KEY, UNSET_PROPERTY);
  }

}
