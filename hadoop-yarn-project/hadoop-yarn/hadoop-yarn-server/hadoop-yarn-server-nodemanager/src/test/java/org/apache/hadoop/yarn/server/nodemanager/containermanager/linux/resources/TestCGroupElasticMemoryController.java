/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for elastic non-strict memory controller based on cgroups.
 */
public class TestCGroupElasticMemoryController {
  protected static final Logger LOG = LoggerFactory
      .getLogger(TestCGroupElasticMemoryController.class);
  private YarnConfiguration conf = new YarnConfiguration();
  private File script = new File("target/" +
      TestCGroupElasticMemoryController.class.getName());

  /**
   * Test that at least one memory type is requested.
   * @throws YarnException on exception
   */
  @Test(expected = YarnException.class)
  public void testConstructorOff()
      throws YarnException {
    new CGroupElasticMemoryController(
        conf,
        null,
        null,
        false,
        false,
        10000
    );
  }

  /**
   * Test that the OOM logic is pluggable.
   * @throws YarnException on exception
   */
  @Test
  public void testConstructorHandler()
      throws YarnException {
    conf.setClass(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_HANDLER,
        DummyRunnableWithContext.class, Runnable.class);
    CGroupsHandler handler = mock(CGroupsHandler.class);
    when(handler.getPathForCGroup(any(), any())).thenReturn("");
    new CGroupElasticMemoryController(
        conf,
        null,
        handler,
        true,
        false,
        10000
    );
  }

  /**
   * Test that the handler is notified about multiple OOM events.
   * @throws Exception on exception
   */
  @Test(timeout = 20000)
  public void testMultipleOOMEvents() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        script.getAbsolutePath());
    try {
      FileUtils.writeStringToFile(script,
          "#!/bin/bash\nprintf oomevent;printf oomevent;\n",
          Charset.defaultCharset(), false);
      assertTrue("Could not set executable",
          script.setExecutable(true));

      CGroupsHandler cgroups = mock(CGroupsHandler.class);
      when(cgroups.getPathForCGroup(any(), any())).thenReturn("");
      when(cgroups.getCGroupParam(any(), any(), any()))
          .thenReturn("under_oom 0");

      Runnable handler = mock(Runnable.class);
      doNothing().when(handler).run();

      CGroupElasticMemoryController controller =
          new CGroupElasticMemoryController(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      controller.run();
      verify(handler, times(2)).run();
    } finally {
      assertTrue(String.format("Could not clean up script %s",
          script.getAbsolutePath()), script.delete());
    }
  }

  /**
   * Test the scenario that the controller is stopped before.
   * the child process starts
   * @throws Exception one exception
   */
  @Test(timeout = 20000)
  public void testStopBeforeStart() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        script.getAbsolutePath());
    try {
      FileUtils.writeStringToFile(script,
          "#!/bin/bash\nprintf oomevent;printf oomevent;\n",
          Charset.defaultCharset(), false);
      assertTrue("Could not set executable",
          script.setExecutable(true));

      CGroupsHandler cgroups = mock(CGroupsHandler.class);
      when(cgroups.getPathForCGroup(any(), any())).thenReturn("");
      when(cgroups.getCGroupParam(any(), any(), any()))
          .thenReturn("under_oom 0");

      Runnable handler = mock(Runnable.class);
      doNothing().when(handler).run();

      CGroupElasticMemoryController controller =
          new CGroupElasticMemoryController(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      controller.stopListening();
      controller.run();
      verify(handler, times(0)).run();
    } finally {
      assertTrue(String.format("Could not clean up script %s",
          script.getAbsolutePath()), script.delete());
    }
  }

  /**
   * Test the edge case that OOM is never resolved.
   * @throws Exception on exception
   */
  @Test(timeout = 20000, expected = YarnRuntimeException.class)
  public void testInfiniteOOM() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        script.getAbsolutePath());
    Runnable handler = mock(Runnable.class);
    try {
      FileUtils.writeStringToFile(script,
          "#!/bin/bash\nprintf oomevent;sleep 1000;\n",
          Charset.defaultCharset(), false);
      assertTrue("Could not set executable",
          script.setExecutable(true));

      CGroupsHandler cgroups = mock(CGroupsHandler.class);
      when(cgroups.getPathForCGroup(any(), any())).thenReturn("");
      when(cgroups.getCGroupParam(any(), any(), any()))
          .thenReturn("under_oom 1");

      doNothing().when(handler).run();

      CGroupElasticMemoryController controller =
          new CGroupElasticMemoryController(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      controller.run();
    } finally {
      verify(handler, times(1)).run();
      assertTrue(String.format("Could not clean up script %s",
          script.getAbsolutePath()), script.delete());
    }
  }

  /**
   * Test the edge case that OOM cannot be resolved due to the lack of
   * containers.
   * @throws Exception on exception
   */
  @Test(timeout = 20000, expected = YarnRuntimeException.class)
  public void testNothingToKill() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        script.getAbsolutePath());
    Runnable handler = mock(Runnable.class);
    try {
      FileUtils.writeStringToFile(script,
          "#!/bin/bash\nprintf oomevent;sleep 1000;\n",
          Charset.defaultCharset(), false);
      assertTrue("Could not set executable",
          script.setExecutable(true));

      CGroupsHandler cgroups = mock(CGroupsHandler.class);
      when(cgroups.getPathForCGroup(any(), any())).thenReturn("");
      when(cgroups.getCGroupParam(any(), any(), any()))
          .thenReturn("under_oom 1");

      doThrow(new YarnRuntimeException("Expected")).when(handler).run();

      CGroupElasticMemoryController controller =
          new CGroupElasticMemoryController(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      controller.run();
    } finally {
      verify(handler, times(1)).run();
      assertTrue(String.format("Could not clean up script %s",
          script.getAbsolutePath()), script.delete());
    }
  }

  /**
   * Test that node manager can exit listening.
   * This is done by running a long running listener for 10000 seconds.
   * Then we wait for 2 seconds and stop listening.
   * We do not use a script this time to avoid leaking the child process.
   * @throws Exception exception occurred
   */
  @Test(timeout = 20000)
  public void testNormalExit() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        "sleep");
    ExecutorService service = Executors.newFixedThreadPool(1);
    try {
      CGroupsHandler cgroups = mock(CGroupsHandler.class);
      // This will be passed to sleep as an argument
      when(cgroups.getPathForCGroup(any(), any())).thenReturn("10000");
      when(cgroups.getCGroupParam(any(), any(), any()))
          .thenReturn("under_oom 0");

      Runnable handler = mock(Runnable.class);
      doNothing().when(handler).run();

      CGroupElasticMemoryController controller =
          new CGroupElasticMemoryController(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      long start = System.currentTimeMillis();
      service.submit(() -> {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ex) {
          assertTrue("Wait interrupted.", false);
        }
        LOG.info(String.format("Calling process destroy in %d ms",
            System.currentTimeMillis() - start));
        controller.stopListening();
        LOG.info("Called process destroy.");
      });
      controller.run();
    } finally {
      service.shutdown();
    }
  }

  /**
   * Test that DefaultOOMHandler is instantiated correctly in
   * the elastic constructor.
   * @throws YarnException Could not set up elastic memory control.
   */
  @Test
  public void testDefaultConstructor() throws YarnException{
    CGroupsHandler handler = mock(CGroupsHandler.class);
    when(handler.getPathForCGroup(any(), any())).thenReturn("");
    new CGroupElasticMemoryController(
        conf, null, handler, true, false, 10);
  }
}
