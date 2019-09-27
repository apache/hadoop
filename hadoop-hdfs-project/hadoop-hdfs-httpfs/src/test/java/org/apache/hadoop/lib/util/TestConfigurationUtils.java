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

import static org.assertj.core.api.Assertions.assertThat;
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
    assertThat(conf.size()).isEqualTo(0);

    byte[] bytes = "<configuration><property><name>a</name><value>A</value></property></configuration>".getBytes();
    InputStream is = new ByteArrayInputStream(bytes);
    conf = new Configuration(false);
    ConfigurationUtils.load(conf, is);
    assertThat(conf.size()).isEqualTo(1);
    assertThat(conf.get("a")).isEqualTo("A");
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

    assertThat(targetConf.get("testParameter1")).isEqualTo("valueFromSource");
    assertThat(targetConf.get("testParameter2")).isEqualTo("valueFromSource");
    assertThat(targetConf.get("testParameter3")).isEqualTo("valueFromTarget");
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

    assertThat(targetConf.get("testParameter1")).isEqualTo("valueFromSource");
    assertThat(targetConf.get("testParameter2")).isEqualTo("originalValueFromTarget");
    assertThat(targetConf.get("testParameter3")).isEqualTo("originalValueFromTarget");

    assertThat(srcConf.get("testParameter1")).isEqualTo("valueFromSource");
    assertThat(srcConf.get("testParameter2")).isEqualTo("valueFromSource");
    assertNull(srcConf.get("testParameter3"));
  }


  @Test
  public void resolve() {
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "${a}");

    assertThat(conf.getRaw("a")).isEqualTo("A");
    assertThat(conf.getRaw("b")).isEqualTo("${a}");
    conf = ConfigurationUtils.resolve(conf);
    assertThat(conf.getRaw("a")).isEqualTo("A");
    assertThat(conf.getRaw("b")).isEqualTo("A");
  }

  @Test
  public void testVarResolutionAndSysProps() {
    String userName = System.getProperty("user.name");
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "${a}");
    conf.set("c", "${user.name}");
    conf.set("d", "${aaa}");
    assertThat(conf.getRaw("a")).isEqualTo("A");
    assertThat(conf.getRaw("b")).isEqualTo("${a}");
    assertThat(conf.getRaw("c")).isEqualTo("${user.name}");
    assertThat(conf.get("a")).isEqualTo("A");
    assertThat(conf.get("b")).isEqualTo("A");
    assertThat(conf.get("c")).isEqualTo(userName);
    assertThat(conf.get("d")).isEqualTo("${aaa}");

    conf.set("user.name", "foo");
    assertThat(conf.get("user.name")).isEqualTo("foo");
  }

}
