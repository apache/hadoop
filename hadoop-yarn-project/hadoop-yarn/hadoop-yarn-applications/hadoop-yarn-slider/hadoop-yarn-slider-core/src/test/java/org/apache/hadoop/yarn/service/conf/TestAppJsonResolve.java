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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.ConfigFile;
import org.apache.slider.api.resource.ConfigFile.TypeEnum;
import org.apache.slider.api.resource.Configuration;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.persist.JsonSerDeser;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.slider.api.InternalKeys.CHAOS_MONKEY_INTERVAL;
import static org.apache.slider.api.InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_DAYS;
import static org.apache.slider.api.InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_HOURS;
import static org.apache.slider.api.InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_MINUTES;
import static org.apache.hadoop.yarn.service.conf.ExampleAppJson.APP_JSON;
import static org.apache.hadoop.yarn.service.conf.ExampleAppJson.EXTERNAL_JSON_1;
import static org.apache.hadoop.yarn.service.conf.ExampleAppJson.OVERRIDE_JSON;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;

/**
 * Test global configuration resolution.
 */
public class TestAppJsonResolve extends Assert {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestAppJsonResolve.class);

  @Test
  public void testOverride() throws Throwable {
    Application orig = ExampleAppJson.loadResource(OVERRIDE_JSON);

    Configuration global = orig.getConfiguration();
    assertEquals("a", global.getProperty("g1"));
    assertEquals("b", global.getProperty("g2"));
    assertEquals(2, global.getFiles().size());

    Configuration simple = orig.getComponent("simple").getConfiguration();
    assertEquals(0, simple.getProperties().size());
    assertEquals(1, simple.getFiles().size());

    Configuration master = orig.getComponent("master").getConfiguration();
    assertEquals("m", master.getProperty("name"));
    assertEquals("overridden", master.getProperty("g1"));
    assertEquals(0, master.getFiles().size());

    Configuration worker = orig.getComponent("worker").getConfiguration();
    LOG.info("worker = {}", worker);
    assertEquals(3, worker.getProperties().size());
    assertEquals(0, worker.getFiles().size());

    assertEquals("worker", worker.getProperty("name"));
    assertEquals("overridden-by-worker", worker.getProperty("g1"));
    assertNull(worker.getProperty("g2"));
    assertEquals("1000", worker.getProperty("timeout"));

    // here is the resolution
    SliderFileSystem sfs = createNiceMock(SliderFileSystem.class);
    FileSystem mockFs = createNiceMock(FileSystem.class);
    expect(sfs.getFileSystem()).andReturn(mockFs).anyTimes();
    expect(sfs.buildClusterDirPath(anyObject())).andReturn(
        new Path("cluster_dir_path")).anyTimes();
    replay(sfs, mockFs);
    ServiceApiUtil.validateAndResolveApplication(orig, sfs, new
        YarnConfiguration());

    global = orig.getConfiguration();
    LOG.info("global = {}", global);
    assertEquals("a", global.getProperty("g1"));
    assertEquals("b", global.getProperty("g2"));
    assertEquals(2, global.getFiles().size());

    simple = orig.getComponent("simple").getConfiguration();
    assertEquals(2, simple.getProperties().size());
    assertEquals("a", simple.getProperty("g1"));
    assertEquals("b", simple.getProperty("g2"));
    assertEquals(2, simple.getFiles().size());

    Set<ConfigFile> files = new HashSet<>();
    Map<String, String> props = new HashMap<>();
    props.put("k1", "overridden");
    props.put("k2", "v2");
    files.add(new ConfigFile().destFile("file1").type(TypeEnum
        .PROPERTIES).props(props));
    files.add(new ConfigFile().destFile("file2").type(TypeEnum
        .XML).props(Collections.singletonMap("k3", "v3")));
    assertTrue(files.contains(simple.getFiles().get(0)));
    assertTrue(files.contains(simple.getFiles().get(1)));

    master = orig.getComponent("master").getConfiguration();
    LOG.info("master = {}", master);
    assertEquals(3, master.getProperties().size());
    assertEquals("m", master.getProperty("name"));
    assertEquals("overridden", master.getProperty("g1"));
    assertEquals("b", master.getProperty("g2"));
    assertEquals(2, master.getFiles().size());

    props.put("k1", "v1");
    files.clear();
    files.add(new ConfigFile().destFile("file1").type(TypeEnum
        .PROPERTIES).props(props));
    files.add(new ConfigFile().destFile("file2").type(TypeEnum
        .XML).props(Collections.singletonMap("k3", "v3")));

    assertTrue(files.contains(master.getFiles().get(0)));
    assertTrue(files.contains(master.getFiles().get(1)));

    worker = orig.getComponent("worker").getConfiguration();
    LOG.info("worker = {}", worker);
    assertEquals(4, worker.getProperties().size());

    assertEquals("worker", worker.getProperty("name"));
    assertEquals("overridden-by-worker", worker.getProperty("g1"));
    assertEquals("b", worker.getProperty("g2"));
    assertEquals("1000", worker.getProperty("timeout"));
    assertEquals(2, worker.getFiles().size());

    assertTrue(files.contains(worker.getFiles().get(0)));
    assertTrue(files.contains(worker.getFiles().get(1)));
  }

  @Test
  public void testOverrideExternalConfiguration() throws IOException {
    Application orig = ExampleAppJson.loadResource(EXTERNAL_JSON_1);

    Configuration global = orig.getConfiguration();
    assertEquals(0, global.getProperties().size());

    assertEquals(3, orig.getComponents().size());

    Configuration simple = orig.getComponent("simple").getConfiguration();
    assertEquals(0, simple.getProperties().size());

    Configuration master = orig.getComponent("master").getConfiguration();
    assertEquals(1, master.getProperties().size());
    assertEquals("is-overridden", master.getProperty("g3"));

    Configuration other = orig.getComponent("other").getConfiguration();
    assertEquals(0, other.getProperties().size());

    // load the external application
    SliderFileSystem sfs = createNiceMock(SliderFileSystem.class);
    FileSystem mockFs = createNiceMock(FileSystem.class);
    expect(sfs.getFileSystem()).andReturn(mockFs).anyTimes();
    expect(sfs.buildClusterDirPath(anyObject())).andReturn(
        new Path("cluster_dir_path")).anyTimes();
    replay(sfs, mockFs);
    Application ext = ExampleAppJson.loadResource(APP_JSON);
    ServiceApiUtil.validateAndResolveApplication(ext, sfs, new
        YarnConfiguration());
    reset(sfs, mockFs);

    // perform the resolution on original application
    JsonSerDeser<Application> jsonSerDeser = createNiceMock(JsonSerDeser
        .class);
    expect(sfs.getFileSystem()).andReturn(mockFs).anyTimes();
    expect(sfs.buildClusterDirPath(anyObject())).andReturn(
        new Path("cluster_dir_path")).anyTimes();
    expect(jsonSerDeser.load(anyObject(), anyObject())).andReturn(ext)
        .anyTimes();
    replay(sfs, mockFs, jsonSerDeser);
    ServiceApiUtil.setJsonSerDeser(jsonSerDeser);
    ServiceApiUtil.validateAndResolveApplication(orig, sfs, new
        YarnConfiguration());

    global = orig.getConfiguration();
    assertEquals(0, global.getProperties().size());

    assertEquals(4, orig.getComponents().size());

    simple = orig.getComponent("simple").getConfiguration();
    assertEquals(3, simple.getProperties().size());
    assertEquals("a", simple.getProperty("g1"));
    assertEquals("b", simple.getProperty("g2"));
    assertEquals("60",
        simple.getProperty("internal.chaos.monkey.interval.seconds"));

    master = orig.getComponent("master").getConfiguration();
    assertEquals(5, master.getProperties().size());
    assertEquals("512M", master.getProperty("jvm.heapsize"));
    assertEquals("overridden", master.getProperty("g1"));
    assertEquals("b", master.getProperty("g2"));
    assertEquals("is-overridden", master.getProperty("g3"));
    assertEquals("60",
        simple.getProperty("internal.chaos.monkey.interval.seconds"));

    Configuration worker = orig.getComponent("worker").getConfiguration();
    LOG.info("worker = {}", worker);
    assertEquals(4, worker.getProperties().size());
    assertEquals("512M", worker.getProperty("jvm.heapsize"));
    assertEquals("overridden-by-worker", worker.getProperty("g1"));
    assertEquals("b", worker.getProperty("g2"));
    assertEquals("60",
        worker.getProperty("internal.chaos.monkey.interval.seconds"));

    other = orig.getComponent("other").getConfiguration();
    assertEquals(0, other.getProperties().size());
  }

  @Test
  public void testTimeIntervalLoading() throws Throwable {
    Application orig = ExampleAppJson.loadResource(APP_JSON);

    Configuration conf = orig.getConfiguration();
    long s = conf.getPropertyLong(
        CHAOS_MONKEY_INTERVAL + SliderUtils.SECONDS,
        0);
    assertEquals(60, s);
    long monkeyInterval = SliderUtils.getTimeRange(conf,
        CHAOS_MONKEY_INTERVAL,
        DEFAULT_CHAOS_MONKEY_INTERVAL_DAYS,
        DEFAULT_CHAOS_MONKEY_INTERVAL_HOURS,
        DEFAULT_CHAOS_MONKEY_INTERVAL_MINUTES,
        0);
    assertEquals(60L, monkeyInterval);
  }
}
