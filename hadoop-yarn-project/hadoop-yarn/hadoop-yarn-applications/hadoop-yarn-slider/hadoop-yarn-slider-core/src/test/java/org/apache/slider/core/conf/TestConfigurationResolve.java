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

import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Configuration;
import org.apache.slider.common.tools.SliderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.slider.api.InternalKeys.CHAOS_MONKEY_INTERVAL;
import static org.apache.slider.api.InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_DAYS;
import static org.apache.slider.api.InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_HOURS;
import static org.apache.slider.api.InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_MINUTES;
import static org.apache.slider.core.conf.ExampleConfResources.APP_JSON;
import static org.apache.slider.core.conf.ExampleConfResources.OVERRIDE_JSON;

/**
 * Test global configuration resolution.
 */
public class TestConfigurationResolve extends Assert {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestConfigurationResolve.class);

  @Test
  public void testOverride() throws Throwable {

    Application orig = ExampleConfResources.loadResource(OVERRIDE_JSON);

    Configuration global = orig.getConfiguration();
    assertEquals("a", global.getProperty("g1"));
    assertEquals("b", global.getProperty("g2"));

    Configuration simple = orig.getComponent("simple").getConfiguration();
    assertEquals(0, simple.getProperties().size());

    Configuration master = orig.getComponent("master").getConfiguration();
    assertEquals("m", master.getProperty("name"));
    assertEquals("overridden", master.getProperty("g1"));

    Configuration worker = orig.getComponent("worker").getConfiguration();
    LOG.info("worker = {}", worker);
    assertEquals(3, worker.getProperties().size());

    assertEquals("worker", worker.getProperty("name"));
    assertEquals("overridden-by-worker", worker.getProperty("g1"));
    assertNull(worker.getProperty("g2"));
    assertEquals("1000", worker.getProperty("timeout"));

    // here is the resolution
    SliderUtils.resolve(orig);

    global = orig.getConfiguration();
    LOG.info("global = {}", global);
    assertEquals("a", global.getProperty("g1"));
    assertEquals("b", global.getProperty("g2"));

    simple = orig.getComponent("simple").getConfiguration();
    assertEquals(2, simple.getProperties().size());
    assertEquals("a", simple.getProperty("g1"));
    assertEquals("b", simple.getProperty("g2"));


    master = orig.getComponent("master").getConfiguration();
    LOG.info("master = {}", master);
    assertEquals(3, master.getProperties().size());
    assertEquals("m", master.getProperty("name"));
    assertEquals("overridden", master.getProperty("g1"));
    assertEquals("b", master.getProperty("g2"));

    worker = orig.getComponent("worker").getConfiguration();
    LOG.info("worker = {}", worker);
    assertEquals(4, worker.getProperties().size());

    assertEquals("worker", worker.getProperty("name"));
    assertEquals("overridden-by-worker", worker.getProperty("g1"));
    assertEquals("b", worker.getProperty("g2"));
    assertEquals("1000", worker.getProperty("timeout"));

  }

  @Test
  public void testTimeIntervalLoading() throws Throwable {

    Application orig = ExampleConfResources.loadResource(APP_JSON);

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
