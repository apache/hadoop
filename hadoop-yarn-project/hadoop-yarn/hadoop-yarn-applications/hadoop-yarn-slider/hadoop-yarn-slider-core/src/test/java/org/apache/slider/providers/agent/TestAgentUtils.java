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

package org.apache.slider.providers.agent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.tools.TestUtility;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAgentUtils {
  protected static final Logger log =
      LoggerFactory.getLogger(TestAgentUtils.class);
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private static final String metainfo_str = "<metainfo>\n"
      + "  <schemaVersion>2.0</schemaVersion>\n"
      + "  <application>\n"
      + "      <name>MYTESTAPPLICATION</name>\n"
      + "      <comment>\n"
      + "        My Test Application\n"
      + "      </comment>\n"
      + "      <version>1.0</version>\n"
      + "      <type>YARN-APP</type>\n"
      + "      <components>\n"
      + "        <component>\n"
      + "          <name>REST</name>\n"
      + "          <category>MASTER</category>\n"
      + "          <commandScript>\n"
      + "            <script>scripts/rest.py</script>\n"
      + "            <scriptType>PYTHON</scriptType>\n"
      + "            <timeout>600</timeout>\n"
      + "          </commandScript>\n"
      + "        </component>\n"
      + "      </components>\n"
      + "  </application>\n"
      + "</metainfo>";

  @Test
  public void testGetApplicationMetainfo() throws Exception {
    String zipFileName = TestUtility.createAppPackage(
        folder,
        "testpkg",
        "test.zip",
        "target/test-classes/org/apache/slider/common/tools/test");
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.getLocal(configuration);
    log.info("fs working dir is {}", fs.getWorkingDirectory().toString());
    SliderFileSystem sliderFileSystem = new SliderFileSystem(fs, configuration);

    // Without accompany metainfo file, read metainfo from the zip file
    Metainfo metainfo = AgentUtils.getApplicationMetainfo(
        sliderFileSystem, zipFileName, false);
    Assert.assertNotNull(metainfo.getApplication());
    Assert.assertEquals("STORM", metainfo.getApplication().getName());

    // With accompany metainfo file, read metainfo from the accompany file
    String acompanyFileName = zipFileName + ".metainfo.xml";
    File f = new File(acompanyFileName);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(f))) {
      writer.write(metainfo_str);
    }
    metainfo = AgentUtils.getApplicationMetainfo(
        sliderFileSystem, zipFileName, false);
    Assert.assertNotNull(metainfo.getApplication());
    Assert.assertEquals("MYTESTAPPLICATION", metainfo.getApplication().getName());
  }
}
