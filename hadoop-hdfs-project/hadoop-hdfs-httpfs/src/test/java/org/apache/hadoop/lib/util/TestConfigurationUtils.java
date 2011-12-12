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

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestConfigurationUtils {

  @Test
  public void constructors() throws Exception {
    Configuration conf = new Configuration(false);
    Assert.assertEquals(conf.size(), 0);

    byte[] bytes = "<configuration><property><name>a</name><value>A</value></property></configuration>".getBytes();
    InputStream is = new ByteArrayInputStream(bytes);
    conf = new Configuration(false);
    ConfigurationUtils.load(conf, is);
    Assert.assertEquals(conf.size(), 1);
    Assert.assertEquals(conf.get("a"), "A");
  }


  @Test(expected = IOException.class)
  public void constructorsFail3() throws Exception {
    InputStream is = new ByteArrayInputStream("<xonfiguration></xonfiguration>".getBytes());
    Configuration conf = new Configuration(false);
    ConfigurationUtils.load(conf, is);
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

    Assert.assertEquals("valueFromSource", targetConf.get("testParameter1"));
    Assert.assertEquals("valueFromSource", targetConf.get("testParameter2"));
    Assert.assertEquals("valueFromTarget", targetConf.get("testParameter3"));
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

    Assert.assertEquals("valueFromSource", targetConf.get("testParameter1"));
    Assert.assertEquals("originalValueFromTarget", targetConf.get("testParameter2"));
    Assert.assertEquals("originalValueFromTarget", targetConf.get("testParameter3"));

    Assert.assertEquals("valueFromSource", srcConf.get("testParameter1"));
    Assert.assertEquals("valueFromSource", srcConf.get("testParameter2"));
    Assert.assertNull(srcConf.get("testParameter3"));
  }


  @Test
  public void resolve() {
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "${a}");
    Assert.assertEquals(conf.getRaw("a"), "A");
    Assert.assertEquals(conf.getRaw("b"), "${a}");
    conf = ConfigurationUtils.resolve(conf);
    Assert.assertEquals(conf.getRaw("a"), "A");
    Assert.assertEquals(conf.getRaw("b"), "A");
  }

  @Test
  public void testVarResolutionAndSysProps() {
    String userName = System.getProperty("user.name");
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "${a}");
    conf.set("c", "${user.name}");
    conf.set("d", "${aaa}");
    Assert.assertEquals(conf.getRaw("a"), "A");
    Assert.assertEquals(conf.getRaw("b"), "${a}");
    Assert.assertEquals(conf.getRaw("c"), "${user.name}");
    Assert.assertEquals(conf.get("a"), "A");
    Assert.assertEquals(conf.get("b"), "A");
    Assert.assertEquals(conf.get("c"), userName);
    Assert.assertEquals(conf.get("d"), "${aaa}");

    conf.set("user.name", "foo");
    Assert.assertEquals(conf.get("user.name"), "foo");
  }

}
