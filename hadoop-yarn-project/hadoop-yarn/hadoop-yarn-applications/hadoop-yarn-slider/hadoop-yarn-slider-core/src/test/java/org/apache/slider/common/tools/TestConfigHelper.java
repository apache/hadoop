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

package org.apache.slider.common.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.slider.utils.YarnMiniClusterTestBase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * Test config helper.
 */
public class TestConfigHelper extends YarnMiniClusterTestBase {

  @Test
  public void testConfigLoaderIteration() throws Throwable {

    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" " +
        "standalone=\"no\"?><configuration><property><name>key</name>" +
        "<value>value</value><source>programatically</source></property>" +
        "</configuration>";
    InputStream ins = new ByteArrayInputStream(xml.getBytes("UTF8"));
    Configuration conf = new Configuration(false);
    conf.addResource(ins);
    Configuration conf2 = new Configuration(false);
    for (Map.Entry<String, String> entry : conf) {
      conf2.set(entry.getKey(), entry.getValue(), "src");
    }

  }

  @Test
  public void testConfigDeprecation() throws Throwable {
    ConfigHelper.registerDeprecatedConfigItems();
    Configuration conf = new Configuration(false);
    // test deprecated items here
  }
}
