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

package org.apache.hadoop.lib.service.instrumentation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.lib.server.Server;
import org.apache.hadoop.lib.service.Instrumentation;
import org.apache.hadoop.lib.service.scheduler.SchedulerService;
import org.apache.hadoop.test.HTestCase;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

public class TestInstrumentationService extends HTestCase {

  @Override
  protected float getWaitForRatio() {
    return 1;
  }

  @Test
  public void cron() {
    InstrumentationService.Cron cron = new InstrumentationService.Cron();
    assertEquals(cron.start, 0);
    assertEquals(cron.lapStart, 0);
    assertEquals(cron.own, 0);
    assertEquals(cron.total, 0);
    long begin = Time.now();
    assertEquals(cron.start(), cron);
    assertEquals(cron.start(), cron);
    assertEquals(cron.start, begin, 20);
    assertEquals(cron.start, cron.lapStart);
    sleep(100);
    assertEquals(cron.stop(), cron);
    long end = Time.now();
    long delta = end - begin;
    assertEquals(cron.own, delta, 20);
    assertEquals(cron.total, 0);
    assertEquals(cron.lapStart, 0);
    sleep(100);
    long reStart = Time.now();
    cron.start();
    assertEquals(cron.start, begin, 20);
    assertEquals(cron.lapStart, reStart, 20);
    sleep(100);
    cron.stop();
    long reEnd = Time.now();
    delta += reEnd - reStart;
    assertEquals(cron.own, delta, 20);
    assertEquals(cron.total, 0);
    assertEquals(cron.lapStart, 0);
    cron.end();
    assertEquals(cron.total, reEnd - begin, 20);

    try {
      cron.start();
      fail();
    } catch (IllegalStateException ex) {
    } catch (Exception ex) {
      fail();
    }

    try {
      cron.stop();
      fail();
    } catch (IllegalStateException ex) {
    } catch (Exception ex) {
      fail();
    }
  }

  @Test
  public void timer() throws Exception {
    InstrumentationService.Timer timer = new InstrumentationService.Timer(2);
    InstrumentationService.Cron cron = new InstrumentationService.Cron();

    long ownStart;
    long ownEnd;
    long totalStart;
    long totalEnd;
    long ownDelta;
    long totalDelta;
    long avgTotal;
    long avgOwn;

    cron.start();
    ownStart = Time.now();
    totalStart = ownStart;
    ownDelta = 0;
    sleep(100);

    cron.stop();
    ownEnd = Time.now();
    ownDelta += ownEnd - ownStart;
    sleep(100);

    cron.start();
    ownStart = Time.now();
    sleep(100);

    cron.stop();
    ownEnd = Time.now();
    ownDelta += ownEnd - ownStart;
    totalEnd = ownEnd;
    totalDelta = totalEnd - totalStart;

    avgTotal = totalDelta;
    avgOwn = ownDelta;

    timer.addCron(cron);
    long[] values = timer.getValues();
    assertEquals(values[InstrumentationService.Timer.LAST_TOTAL], totalDelta, 20);
    assertEquals(values[InstrumentationService.Timer.LAST_OWN], ownDelta, 20);
    assertEquals(values[InstrumentationService.Timer.AVG_TOTAL], avgTotal, 20);
    assertEquals(values[InstrumentationService.Timer.AVG_OWN], avgOwn, 20);

    cron = new InstrumentationService.Cron();

    cron.start();
    ownStart = Time.now();
    totalStart = ownStart;
    ownDelta = 0;
    sleep(200);

    cron.stop();
    ownEnd = Time.now();
    ownDelta += ownEnd - ownStart;
    sleep(200);

    cron.start();
    ownStart = Time.now();
    sleep(200);

    cron.stop();
    ownEnd = Time.now();
    ownDelta += ownEnd - ownStart;
    totalEnd = ownEnd;
    totalDelta = totalEnd - totalStart;

    avgTotal = (avgTotal * 1 + totalDelta) / 2;
    avgOwn = (avgOwn * 1 + ownDelta) / 2;

    timer.addCron(cron);
    values = timer.getValues();
    assertEquals(values[InstrumentationService.Timer.LAST_TOTAL], totalDelta, 20);
    assertEquals(values[InstrumentationService.Timer.LAST_OWN], ownDelta, 20);
    assertEquals(values[InstrumentationService.Timer.AVG_TOTAL], avgTotal, 20);
    assertEquals(values[InstrumentationService.Timer.AVG_OWN], avgOwn, 20);

    avgTotal = totalDelta;
    avgOwn = ownDelta;

    cron = new InstrumentationService.Cron();

    cron.start();
    ownStart = Time.now();
    totalStart = ownStart;
    ownDelta = 0;
    sleep(300);

    cron.stop();
    ownEnd = Time.now();
    ownDelta += ownEnd - ownStart;
    sleep(300);

    cron.start();
    ownStart = Time.now();
    sleep(300);

    cron.stop();
    ownEnd = Time.now();
    ownDelta += ownEnd - ownStart;
    totalEnd = ownEnd;
    totalDelta = totalEnd - totalStart;

    avgTotal = (avgTotal * 1 + totalDelta) / 2;
    avgOwn = (avgOwn * 1 + ownDelta) / 2;

    cron.stop();
    timer.addCron(cron);
    values = timer.getValues();
    assertEquals(values[InstrumentationService.Timer.LAST_TOTAL], totalDelta, 20);
    assertEquals(values[InstrumentationService.Timer.LAST_OWN], ownDelta, 20);
    assertEquals(values[InstrumentationService.Timer.AVG_TOTAL], avgTotal, 20);
    assertEquals(values[InstrumentationService.Timer.AVG_OWN], avgOwn, 20);

    JSONObject json = (JSONObject) new JSONParser().parse(timer.toJSONString());
    assertEquals(json.size(), 4);
    assertEquals(json.get("lastTotal"), values[InstrumentationService.Timer.LAST_TOTAL]);
    assertEquals(json.get("lastOwn"), values[InstrumentationService.Timer.LAST_OWN]);
    assertEquals(json.get("avgTotal"), values[InstrumentationService.Timer.AVG_TOTAL]);
    assertEquals(json.get("avgOwn"), values[InstrumentationService.Timer.AVG_OWN]);

    StringWriter writer = new StringWriter();
    timer.writeJSONString(writer);
    writer.close();
    json = (JSONObject) new JSONParser().parse(writer.toString());
    assertEquals(json.size(), 4);
    assertEquals(json.get("lastTotal"), values[InstrumentationService.Timer.LAST_TOTAL]);
    assertEquals(json.get("lastOwn"), values[InstrumentationService.Timer.LAST_OWN]);
    assertEquals(json.get("avgTotal"), values[InstrumentationService.Timer.AVG_TOTAL]);
    assertEquals(json.get("avgOwn"), values[InstrumentationService.Timer.AVG_OWN]);
  }

  @Test
  public void sampler() throws Exception {
    final long value[] = new long[1];
    Instrumentation.Variable<Long> var = new Instrumentation.Variable<Long>() {
      @Override
      public Long getValue() {
        return value[0];
      }
    };

    InstrumentationService.Sampler sampler = new InstrumentationService.Sampler();
    sampler.init(4, var);
    assertEquals(sampler.getRate(), 0f, 0.0001);
    sampler.sample();
    assertEquals(sampler.getRate(), 0f, 0.0001);
    value[0] = 1;
    sampler.sample();
    assertEquals(sampler.getRate(), (0d + 1) / 2, 0.0001);
    value[0] = 2;
    sampler.sample();
    assertEquals(sampler.getRate(), (0d + 1 + 2) / 3, 0.0001);
    value[0] = 3;
    sampler.sample();
    assertEquals(sampler.getRate(), (0d + 1 + 2 + 3) / 4, 0.0001);
    value[0] = 4;
    sampler.sample();
    assertEquals(sampler.getRate(), (4d + 1 + 2 + 3) / 4, 0.0001);

    JSONObject json = (JSONObject) new JSONParser().parse(sampler.toJSONString());
    assertEquals(json.size(), 2);
    assertEquals(json.get("sampler"), sampler.getRate());
    assertEquals(json.get("size"), 4L);

    StringWriter writer = new StringWriter();
    sampler.writeJSONString(writer);
    writer.close();
    json = (JSONObject) new JSONParser().parse(writer.toString());
    assertEquals(json.size(), 2);
    assertEquals(json.get("sampler"), sampler.getRate());
    assertEquals(json.get("size"), 4L);
  }

  @Test
  public void variableHolder() throws Exception {
    InstrumentationService.VariableHolder<String> variableHolder =
      new InstrumentationService.VariableHolder<String>();

    variableHolder.var = new Instrumentation.Variable<String>() {
      @Override
      public String getValue() {
        return "foo";
      }
    };

    JSONObject json = (JSONObject) new JSONParser().parse(variableHolder.toJSONString());
    assertEquals(json.size(), 1);
    assertEquals(json.get("value"), "foo");

    StringWriter writer = new StringWriter();
    variableHolder.writeJSONString(writer);
    writer.close();
    json = (JSONObject) new JSONParser().parse(writer.toString());
    assertEquals(json.size(), 1);
    assertEquals(json.get("value"), "foo");
  }

  @Test
  @TestDir
  @SuppressWarnings("unchecked")
  public void service() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",", Arrays.asList(InstrumentationService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();

    Instrumentation instrumentation = server.get(Instrumentation.class);
    assertNotNull(instrumentation);
    instrumentation.incr("g", "c", 1);
    instrumentation.incr("g", "c", 2);
    instrumentation.incr("g", "c1", 2);

    Instrumentation.Cron cron = instrumentation.createCron();
    cron.start();
    sleep(100);
    cron.stop();
    instrumentation.addCron("g", "t", cron);
    cron = instrumentation.createCron();
    cron.start();
    sleep(200);
    cron.stop();
    instrumentation.addCron("g", "t", cron);

    Instrumentation.Variable<String> var = new Instrumentation.Variable<String>() {
      @Override
      public String getValue() {
        return "foo";
      }
    };
    instrumentation.addVariable("g", "v", var);

    Instrumentation.Variable<Long> varToSample = new Instrumentation.Variable<Long>() {
      @Override
      public Long getValue() {
        return 1L;
      }
    };
    instrumentation.addSampler("g", "s", 10, varToSample);

    Map<String, ?> snapshot = instrumentation.getSnapshot();
    assertNotNull(snapshot.get("os-env"));
    assertNotNull(snapshot.get("sys-props"));
    assertNotNull(snapshot.get("jvm"));
    assertNotNull(snapshot.get("counters"));
    assertNotNull(snapshot.get("timers"));
    assertNotNull(snapshot.get("variables"));
    assertNotNull(snapshot.get("samplers"));
    assertNotNull(((Map<String, String>) snapshot.get("os-env")).get("PATH"));
    assertNotNull(((Map<String, String>) snapshot.get("sys-props")).get("java.version"));
    assertNotNull(((Map<String, ?>) snapshot.get("jvm")).get("free.memory"));
    assertNotNull(((Map<String, ?>) snapshot.get("jvm")).get("max.memory"));
    assertNotNull(((Map<String, ?>) snapshot.get("jvm")).get("total.memory"));
    assertNotNull(((Map<String, Map<String, Object>>) snapshot.get("counters")).get("g"));
    assertNotNull(((Map<String, Map<String, Object>>) snapshot.get("timers")).get("g"));
    assertNotNull(((Map<String, Map<String, Object>>) snapshot.get("variables")).get("g"));
    assertNotNull(((Map<String, Map<String, Object>>) snapshot.get("samplers")).get("g"));
    assertNotNull(((Map<String, Map<String, Object>>) snapshot.get("counters")).get("g").get("c"));
    assertNotNull(((Map<String, Map<String, Object>>) snapshot.get("counters")).get("g").get("c1"));
    assertNotNull(((Map<String, Map<String, Object>>) snapshot.get("timers")).get("g").get("t"));
    assertNotNull(((Map<String, Map<String, Object>>) snapshot.get("variables")).get("g").get("v"));
    assertNotNull(((Map<String, Map<String, Object>>) snapshot.get("samplers")).get("g").get("s"));

    StringWriter writer = new StringWriter();
    JSONObject.writeJSONString(snapshot, writer);
    writer.close();
    server.destroy();
  }

  @Test
  @TestDir
  @SuppressWarnings("unchecked")
  public void sampling() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",", Arrays.asList(InstrumentationService.class.getName(),
                                                          SchedulerService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    Instrumentation instrumentation = server.get(Instrumentation.class);

    final AtomicInteger count = new AtomicInteger();

    Instrumentation.Variable<Long> varToSample = new Instrumentation.Variable<Long>() {
      @Override
      public Long getValue() {
        return (long) count.incrementAndGet();
      }
    };
    instrumentation.addSampler("g", "s", 10, varToSample);

    sleep(2000);
    int i = count.get();
    assertTrue(i > 0);

    Map<String, Map<String, ?>> snapshot = instrumentation.getSnapshot();
    Map<String, Map<String, Object>> samplers = (Map<String, Map<String, Object>>) snapshot.get("samplers");
    InstrumentationService.Sampler sampler = (InstrumentationService.Sampler) samplers.get("g").get("s");
    assertTrue(sampler.getRate() > 0);

    server.destroy();
  }

}
