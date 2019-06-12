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

package org.apache.hadoop.mapreduce.util;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MRResource;
import org.apache.hadoop.mapreduce.MRResourceType;
import org.apache.hadoop.mapreduce.MRResourceVisibility;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ResourceWithVisibilitySetting;
import org.junit.Test;

/**
 * Test class for MRResourceUtil.
 */
public class TestMRResourceUtil {

  @Test
  public void testGetJobJarWithDefaultVis() {
    // Default visibility
    String jobJarPathStr = "/jobjar";
    Configuration conf = new Configuration();
    conf.set(JobContext.JAR, jobJarPathStr);
    MRResource jobJar = MRResourceUtil.getJobJar(conf);
    assertMRResource(
        MRResourceType.JOBJAR, jobJarPathStr,
        MRJobConfig.JOBJAR_VISIBILITY_DEFAULT, jobJar);
  }

  @Test
  public void testGetJobJarWithCustomVis() {
    // Default visibility
    String jobJarPathStr = "/jobjar1";
    Configuration conf = new Configuration();
    conf.set(JobContext.JAR, jobJarPathStr);
    conf.set(
        MRJobConfig.JOBJAR_VISIBILITY, MRResourceVisibility.PRIVATE.name());
    MRResource jobJar = MRResourceUtil.getJobJar(conf);
    assertMRResource(
        MRResourceType.JOBJAR, jobJarPathStr,
        MRResourceVisibility.PRIVATE, jobJar);
  }

  @Test
  public void testGetResourceFromMRConfigLibJar() {
    Configuration conf = new Configuration();
    String path0 = "jar0";
    appendToConfig(conf, GenericOptionsParser.TMP_LIBJARS_CONF_KEY, path0);
    String path1 = "jar1";
    MRResourceVisibility path1Vis = MRResourceVisibility.PUBLIC;
    appendToConfig(conf, GenericOptionsParser.TMP_LIBJARS_CONF_KEY,
        path1
            + ResourceWithVisibilitySetting.PATH_SETTING_DELIMITER
            + path1Vis.name());
    String path2 = "jar2";
    MRResourceVisibility path2Vis = MRResourceVisibility.PRIVATE;
    appendToConfig(conf, MRJobConfig.FILES_FOR_CLASSPATH_AND_SHARED_CACHE,
        path2 + ResourceWithVisibilitySetting.PATH_SETTING_DELIMITER
            + path2Vis.name());
    List<MRResource> resourceList =
        MRResourceUtil.getResourceFromMRConfig(MRResourceType.LIBJAR, conf);
    assertEquals(3, resourceList.size());
    assertMRResource(MRResourceType.LIBJAR, path0,
        MRJobConfig.LIBJARS_VISIBILITY_DEFAULT, resourceList.get(0));
    assertMRResource(
        MRResourceType.LIBJAR, path1, path1Vis, resourceList.get(1));
    assertMRResource(
        MRResourceType.LIBJAR, path2, path2Vis, resourceList.get(2));
  }

  @Test
  public void testGetResourceFromMRConfigFiles() {
    Configuration conf = new Configuration();
    String path0 = "file0";
    appendToConfig(conf, GenericOptionsParser.TMP_FILES_CONF_KEY, path0);
    String path1 = "file1";
    MRResourceVisibility path1Vis = MRResourceVisibility.PRIVATE;
    appendToConfig(conf, GenericOptionsParser.TMP_FILES_CONF_KEY,
        path1 + ResourceWithVisibilitySetting.PATH_SETTING_DELIMITER
            + path1Vis.name());
    String path2 = "file2";
    MRResourceVisibility path2Vis = MRResourceVisibility.APPLICATION;
    appendToConfig(conf, GenericOptionsParser.TMP_FILES_CONF_KEY,
        path2 + ResourceWithVisibilitySetting.PATH_SETTING_DELIMITER
            + path2Vis.name());
    List<MRResource> resourceList =
        MRResourceUtil.getResourceFromMRConfig(MRResourceType.FILE, conf);
    assertEquals(3, resourceList.size());
    assertMRResource(MRResourceType.FILE, path0,
        MRJobConfig.FILES_VISIBILITY_DEFAULT, resourceList.get(0));
    assertMRResource(MRResourceType.FILE, path1, path1Vis, resourceList.get(1));
    assertMRResource(MRResourceType.FILE, path2, path2Vis, resourceList.get(2));
  }

  @Test
  public void testGetResourceFromMRConfigArchives() {
    Configuration conf = new Configuration();
    String path0 = "archive0";
    appendToConfig(conf, GenericOptionsParser.TMP_ARCHIVES_CONF_KEY, path0);
    String path1 = "archive1";
    MRResourceVisibility path1Vis = MRResourceVisibility.PUBLIC;
    appendToConfig(conf, GenericOptionsParser.TMP_ARCHIVES_CONF_KEY,
        path1 + ResourceWithVisibilitySetting.PATH_SETTING_DELIMITER
            + path1Vis.name());
    String path2 = "archive2";
    MRResourceVisibility path2Vis = MRResourceVisibility.APPLICATION;
    appendToConfig(conf, GenericOptionsParser.TMP_ARCHIVES_CONF_KEY,
        path2 + ResourceWithVisibilitySetting.PATH_SETTING_DELIMITER
            + path2Vis.name());
    List<MRResource> resourceList =
        MRResourceUtil.getResourceFromMRConfig(MRResourceType.ARCHIVE, conf);
    assertEquals(3, resourceList.size());
    assertMRResource(MRResourceType.ARCHIVE, path0,
        MRJobConfig.ARCHIEVES_VISIBILITY_DEFAULT, resourceList.get(0));
    assertMRResource(
        MRResourceType.ARCHIVE, path1, path1Vis, resourceList.get(1));
    assertMRResource(
        MRResourceType.ARCHIVE, path2, path2Vis, resourceList.get(2));
  }

  private void appendToConfig(
      Configuration conf, String key, String valToAppend) {
    String val = conf.get(key);
    val = val == null ? valToAppend : val + "," + valToAppend;
    conf.set(key, val);
  }

  private void assertMRResource(
      MRResourceType expectType, String expectPathStr,
      MRResourceVisibility expectVis, MRResource resource) {
    assertTrue(expectPathStr.equals(resource.getResourcePathStr()));
    assertTrue(expectType.name().equals(
        resource.getResourceType().name()));
    assertTrue(expectVis.name().equals(
        resource.getResourceVisibility().name()));
  }
}
