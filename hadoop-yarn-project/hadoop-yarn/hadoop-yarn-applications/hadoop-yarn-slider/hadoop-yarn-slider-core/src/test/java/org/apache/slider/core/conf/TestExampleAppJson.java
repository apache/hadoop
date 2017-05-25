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

package org.apache.slider.core.conf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.slider.api.resource.Application;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.util.ServiceApiUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.slider.utils.SliderTestUtils.JSON_SER_DESER;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

/**
 * Test loading example resources.
 */
@RunWith(value = Parameterized.class)
public class TestExampleAppJson extends Assert {
  private String resource;

  public TestExampleAppJson(String resource) {
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
      Application application = JSON_SER_DESER.fromResource(resource);

      SliderFileSystem sfs = createNiceMock(SliderFileSystem.class);
      FileSystem mockFs = createNiceMock(FileSystem.class);
      expect(sfs.getFileSystem()).andReturn(mockFs).anyTimes();
      expect(sfs.buildClusterDirPath(anyObject())).andReturn(
          new Path("cluster_dir_path")).anyTimes();
      replay(sfs, mockFs);

      ServiceApiUtil.validateAndResolveApplication(application, sfs);
    } catch (Exception e) {
      throw new Exception("exception loading " + resource + ":" + e.toString());
    }
  }
}
