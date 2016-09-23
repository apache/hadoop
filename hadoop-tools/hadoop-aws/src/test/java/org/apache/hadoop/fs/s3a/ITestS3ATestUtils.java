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
import static org.apache.hadoop.fs.s3a.scale.S3AScaleTestBase.KEY_SCALE_TESTS_ENABLED;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;

/**
 * Test the test utils. Why an integration test: it's needed to
 * verify property pushdown.
 */
public class ITestS3ATestUtils extends Assert {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ATestUtils.class);

  @Test
  public void testGetTestPropertyLong() throws Throwable {
    Configuration conf = new Configuration(false);
    String key = "undefined.property";
    System.clearProperty(key);
    assertEquals(1, getTestPropertyLong(conf, key, 1));
    conf.setInt(key, 2);
    assertEquals(2, getTestPropertyLong(conf, key, 1));
    System.setProperty(key, "3");
    assertEquals(3, getTestPropertyLong(conf, key, 1));
  }

  @Test
  public void testGetTestPropertyInt() throws Throwable {
    Configuration conf = new Configuration(false);
    String key = "undefined.property";
    System.clearProperty(key);
    assertEquals(1, getTestPropertyInt(conf, key, 1));
    conf.setInt(key, 2);
    assertEquals(2, getTestPropertyInt(conf, key, 1));
    System.setProperty(key, "3");
    assertEquals(3, getTestPropertyInt(conf, key, 1));
    conf.unset(key);
    assertEquals(3, getTestPropertyInt(conf, key, 1));
  }

  @Test
  public void testGetTestPropertyBool() throws Throwable {
    Configuration conf = new Configuration(false);
    String key = "undefined.property";
    System.clearProperty(key);
    assertTrue(getTestPropertyBool(conf, key, true));
    conf.setBoolean(key, false);
    assertFalse(getTestPropertyBool(conf, key, true));
    System.setProperty(key, "true");
    assertTrue(getTestPropertyBool(conf, key, true));
  }

  @Test
  public void testMavenPassdown() throws Throwable {
    Properties props = System.getProperties();
    List<String> keys = new ArrayList<>(props.stringPropertyNames());
    java.util.Collections.sort(keys);
    for (String key : keys) {
      if (key.contains("test") && !key.contains("surefire")) {
        LOG.info("{} = {}", key, props.getProperty(key));
      }
    }
    String p = props.getProperty(KEY_SCALE_TESTS_ENABLED);
    assertNotNull("Not set " + KEY_SCALE_TESTS_ENABLED, p);
    assertTrue("Not a boolean: " + p, "true".equals(p) || "false".equals(p));
  }

}
