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

package org.apache.hadoop.yarn.service.conf;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.yarn.service.ServiceTestUtils.JSON_SER_DESER;

/**
 * Test loading example resources.
 */
@RunWith(value = Parameterized.class)
public class TestLoadExampleAppJson extends Assert {
  private String resource;

  public TestLoadExampleAppJson(String resource) {
    this.resource = resource;
  }

  @Parameterized.Parameters
  public static Collection<String[]> filenames() {
    String[][] stringArray = new String[ExampleAppJson
        .ALL_EXAMPLE_RESOURCES.size()][1];
    int i = 0;
    for (String s : ExampleAppJson.ALL_EXAMPLE_RESOURCES) {
      stringArray[i++][0] = s;
    }
    return Arrays.asList(stringArray);
  }

  @Test
  public void testLoadResource() throws Throwable {
    try {
      Service service = JSON_SER_DESER.fromResource(resource);

      SliderFileSystem sfs = ServiceTestUtils.initMockFs();

      ServiceApiUtil.validateAndResolveService(service, sfs,
          new YarnConfiguration());
    } catch (Exception e) {
      throw new Exception("exception loading " + resource + ":" + e.toString());
    }
  }
}
