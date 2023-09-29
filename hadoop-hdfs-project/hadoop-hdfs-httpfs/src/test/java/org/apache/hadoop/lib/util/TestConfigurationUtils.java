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

package org.apache.hadoop.lib.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestConfigurationUtils {

  @Test
  public void constructors() throws Exception {
    Configuration conf = new Configuration(false);
    assertEquals(conf.size(), 0);

    byte[] bytes = "<configuration><property><name>a</name><value>A</value></property></configuration>".getBytes();
    InputStream is = new ByteArrayInputStream(bytes);
    conf = new Configuration(false);
    ConfigurationUtils.load(conf, is);
    assertEquals(conf.size(), 1);
    assertEquals(conf.get("a"), "A");
  }


  @Test
  public void constructors3() throws Exception {
    InputStream is = new ByteArrayInputStream(
        "<xxx><property name=\"key1\" value=\"val1\"/></xxx>".getBytes());
    Configuration conf = new Configuration(false);
    ConfigurationUtils.load(conf, is);
    assertEquals("val1", conf.get("key1"));
  }

  @Test
  public void copy() throws Exception {
    Configuration srcConf = new Configuration(false);
    Configuration targetConf = new Configuration(false);

    srcConf.set("testParameter1", "valueFromSource");
    srcConf.set("testParameter2", "valueFromSource");

    targetConf.set("testParameter2", "valueFromTarget");
    targetConf.set("testParameter3", "valueFromTarget");

    ConfigurationUtils.copy(srcConf, targetConf);

    assertEquals("valueFromSource", targetConf.get("testParameter1"));
    assertEquals("valueFromSource", targetConf.get("testParameter2"));
    assertEquals("valueFromTarget", targetConf.get("testParameter3"));
  }

  @Test
  public void injectDefaults() throws Exception {
    Configuration srcConf = new Configuration(false);
    Configuration targetConf = new Configuration(false);

    srcConf.set("testParameter1", "valueFromSource");
    srcConf.set("testParameter2", "valueFromSource");

    targetConf.set("testParameter2", "originalValueFromTarget");
    targetConf.set("testParameter3", "originalValueFromTarget");

    ConfigurationUtils.injectDefaults(srcConf, targetConf);

    assertEquals("valueFromSource", targetConf.get("testParameter1"));
    assertEquals("originalValueFromTarget", targetConf.get("testParameter2"));
    assertEquals("originalValueFromTarget", targetConf.get("testParameter3"));

    assertEquals("valueFromSource", srcConf.get("testParameter1"));
    assertEquals("valueFromSource", srcConf.get("testParameter2"));
    assertNull(srcConf.get("testParameter3"));
  }


  @Test
  public void resolve() {
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "${a}");
    assertEquals(conf.getRaw("a"), "A");
    assertEquals(conf.getRaw("b"), "${a}");
    conf = ConfigurationUtils.resolve(conf);
    assertEquals(conf.getRaw("a"), "A");
    assertEquals(conf.getRaw("b"), "A");
  }

  @Test
  public void testVarResolutionAndSysProps() {
    String userName = System.getProperty("user.name");
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "${a}");
    conf.set("c", "${user.name}");
    conf.set("d", "${aaa}");
    assertEquals(conf.getRaw("a"), "A");
    assertEquals(conf.getRaw("b"), "${a}");
    assertEquals(conf.getRaw("c"), "${user.name}");
    assertEquals(conf.get("a"), "A");
    assertEquals(conf.get("b"), "A");
    assertEquals(conf.get("c"), userName);
    assertEquals(conf.get("d"), "${aaa}");

    conf.set("user.name", "foo");
    assertEquals(conf.get("user.name"), "foo");
  }

  @Test
  public void testCompactFormatProperty() throws IOException {
    final String testfile = "test-compact-format-property.xml";
    Configuration conf = new Configuration(false);
    assertEquals(0, conf.size());
    ConfigurationUtils.load(conf,
        Thread.currentThread()
            .getContextClassLoader().getResource(testfile).openStream());
    assertEquals(2, conf.size());
    assertEquals("val1", conf.get("key.1"));
    assertEquals("val2", conf.get("key.2"));
  }
}
