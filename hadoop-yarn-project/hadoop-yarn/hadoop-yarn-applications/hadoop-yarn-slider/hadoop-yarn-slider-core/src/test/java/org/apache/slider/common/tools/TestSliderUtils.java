/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.slider.tools.TestUtility;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/** Test slider util methods. */
public class TestSliderUtils {
  protected static final Logger log =
      LoggerFactory.getLogger(TestSliderUtils.class);
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testGetMetaInfoStreamFromZip() throws Exception {
    String zipFileName = TestUtility.createAppPackage(
        folder,
        "testpkg",
        "test.zip",
        "target/test-classes/org/apache/slider/common/tools/test");
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.getLocal(configuration);
    log.info("fs working dir is {}", fs.getWorkingDirectory().toString());
    SliderFileSystem sliderFileSystem = new SliderFileSystem(fs, configuration);

    InputStream stream = SliderUtils.getApplicationResourceInputStream(
        sliderFileSystem.getFileSystem(),
        new Path(zipFileName),
        "metainfo.xml");
    Assert.assertTrue(stream != null);
    Assert.assertTrue(stream.available() > 0);
  }

  @Test
  public void testTruncate() {
    Assert.assertEquals(SliderUtils.truncate(null, 5), null);
    Assert.assertEquals(SliderUtils.truncate("323", -1), "323");
    Assert.assertEquals(SliderUtils.truncate("3232", 5), "3232");
    Assert.assertEquals(SliderUtils.truncate("1234567890", 0), "1234567890");
    Assert.assertEquals(SliderUtils.truncate("123456789012345", 15), "123456789012345");
    Assert.assertEquals(SliderUtils.truncate("123456789012345", 14), "12345678901...");
    Assert.assertEquals(SliderUtils.truncate("1234567890", 1), "1");
    Assert.assertEquals(SliderUtils.truncate("1234567890", 10), "1234567890");
    Assert.assertEquals(SliderUtils.truncate("", 10), "");
  }

  @Test
  public void testApplicationReportComparison() {
    List<ApplicationReport> instances = getApplicationReports();

    SliderUtils.sortApplicationsByMostRecent(instances);

    Assert.assertEquals(1000, instances.get(0).getStartTime());
    Assert.assertEquals(1000, instances.get(1).getStartTime());
    Assert.assertEquals(1000, instances.get(2).getStartTime());
    Assert.assertEquals(1000, instances.get(3).getStartTime());

    instances = getApplicationReports();

    SliderUtils.sortApplicationReport(instances);
    Assert.assertEquals(1000, instances.get(0).getStartTime());
    Assert.assertEquals(1000, instances.get(1).getStartTime());
    Assert.assertEquals(1000, instances.get(2).getStartTime());
    Assert.assertEquals(1000, instances.get(3).getStartTime());

    Assert.assertTrue(instances.get(0).getYarnApplicationState() == YarnApplicationState.ACCEPTED ||
                      instances.get(0).getYarnApplicationState() == YarnApplicationState.RUNNING);
    Assert.assertTrue(instances.get(1).getYarnApplicationState() == YarnApplicationState.ACCEPTED ||
                      instances.get(1).getYarnApplicationState() == YarnApplicationState.RUNNING);
    Assert.assertTrue(instances.get(2).getYarnApplicationState() == YarnApplicationState.ACCEPTED ||
                      instances.get(2).getYarnApplicationState() == YarnApplicationState.RUNNING);
    Assert.assertTrue(instances.get(3).getYarnApplicationState() == YarnApplicationState.KILLED);
  }

  private List<ApplicationReport> getApplicationReports() {
    List<ApplicationReport> instances = new ArrayList<ApplicationReport>();
    instances.add(getApplicationReport(1000, 0, "app1", YarnApplicationState.ACCEPTED));
    instances.add(getApplicationReport(900, 998, "app1", YarnApplicationState.KILLED));
    instances.add(getApplicationReport(900, 998, "app2", YarnApplicationState.FAILED));
    instances.add(getApplicationReport(1000, 0, "app2", YarnApplicationState.RUNNING));
    instances.add(getApplicationReport(800, 837, "app3", YarnApplicationState.FINISHED));
    instances.add(getApplicationReport(1000, 0, "app3", YarnApplicationState.RUNNING));
    instances.add(getApplicationReport(900, 998, "app3", YarnApplicationState.KILLED));
    instances.add(getApplicationReport(800, 837, "app4", YarnApplicationState.FINISHED));
    instances.add(getApplicationReport(1000, 1050, "app4", YarnApplicationState.KILLED));
    instances.add(getApplicationReport(900, 998, "app4", YarnApplicationState.FINISHED));

    Assert.assertEquals("app1", instances.get(0).getApplicationType());
    Assert.assertEquals("app1", instances.get(1).getApplicationType());
    Assert.assertEquals("app2", instances.get(2).getApplicationType());
    Assert.assertEquals("app2", instances.get(3).getApplicationType());
    return instances;
  }

  private ApplicationReportPBImpl getApplicationReport(long startTime,
                                                       long finishTime,
                                                       String name,
                                                       YarnApplicationState state) {
    ApplicationReportPBImpl ar = new ApplicationReportPBImpl();
    ar.setFinishTime(finishTime);
    ar.setStartTime(startTime);
    ar.setApplicationType(name);
    ar.setYarnApplicationState(state);
    return ar;
  }


  @Test
  public void testGetHdpVersion() {
    String hdpVersion = "2.3.2.0-2766";
    Assert.assertEquals("Version should be empty", null,
        SliderUtils.getHdpVersion());
  }

  @Test
  public void testIsHdp() {
    Assert.assertFalse("Should be false", SliderUtils.isHdp());
  }

  @Test
  public void testWrite() throws IOException {
    File testWriteFile = folder.newFile("testWrite");
    SliderUtils.write(testWriteFile, "test".getBytes("UTF-8"), true);
    Assert.assertTrue(FileUtils.readFileToString(testWriteFile, "UTF-8").equals("test"));
  }
}
