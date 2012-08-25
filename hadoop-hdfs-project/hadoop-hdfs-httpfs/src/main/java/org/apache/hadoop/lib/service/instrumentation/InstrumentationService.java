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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.lib.server.BaseService;
import org.apache.hadoop.lib.server.ServiceException;
import org.apache.hadoop.lib.service.Instrumentation;
import org.apache.hadoop.lib.service.Scheduler;
import org.apache.hadoop.util.Time;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@InterfaceAudience.Private
public class InstrumentationService extends BaseService implements Instrumentation {
  public static final String PREFIX = "instrumentation";
  public static final String CONF_TIMERS_SIZE = "timers.size";

  private int timersSize;
  private Lock counterLock;
  private Lock timerLock;
  private Lock variableLock;
  private Lock samplerLock;
  private Map<String, Map<String, AtomicLong>> counters;
  private Map<String, Map<String, Timer>> timers;
  private Map<String, Map<String, VariableHolder>> variables;
  private Map<String, Map<String, Sampler>> samplers;
  private List<Sampler> samplersList;
  private Map<String, Map<String, ?>> all;

  public InstrumentationService() {
    super(PREFIX);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init() throws ServiceException {
    timersSize = getServiceConfig().getInt(CONF_TIMERS_SIZE, 10);
    counterLock = new ReentrantLock();
    timerLock = new ReentrantLock();
    variableLock = new ReentrantLock();
    samplerLock = new ReentrantLock();
    Map<String, VariableHolder> jvmVariables = new ConcurrentHashMap<String, VariableHolder>();
    counters = new ConcurrentHashMap<String, Map<String, AtomicLong>>();
    timers = new ConcurrentHashMap<String, Map<String, Timer>>();
    variables = new ConcurrentHashMap<String, Map<String, VariableHolder>>();
    samplers = new ConcurrentHashMap<String, Map<String, Sampler>>();
    samplersList = new ArrayList<Sampler>();
    all = new LinkedHashMap<String, Map<String, ?>>();
    all.put("os-env", System.getenv());
    all.put("sys-props", (Map<String, ?>) (Map) System.getProperties());
    all.put("jvm", jvmVariables);
    all.put("counters", (Map) counters);
    all.put("timers", (Map) timers);
    all.put("variables", (Map) variables);
    all.put("samplers", (Map) samplers);

    jvmVariables.put("free.memory", new VariableHolder<Long>(new Instrumentation.Variable<Long>() {
      @Override
      public Long getValue() {
        return Runtime.getRuntime().freeMemory();
      }
    }));
    jvmVariables.put("max.memory", new VariableHolder<Long>(new Instrumentation.Variable<Long>() {
      @Override
      public Long getValue() {
        return Runtime.getRuntime().maxMemory();
      }
    }));
    jvmVariables.put("total.memory", new VariableHolder<Long>(new Instrumentation.Variable<Long>() {
      @Override
      public Long getValue() {
        return Runtime.getRuntime().totalMemory();
      }
    }));
  }

  @Override
  public void postInit() throws ServiceException {
    Scheduler scheduler = getServer().get(Scheduler.class);
    if (scheduler != null) {
      scheduler.schedule(new SamplersRunnable(), 0, 1, TimeUnit.SECONDS);
    }
  }

  @Override
  public Class getInterface() {
    return Instrumentation.class;
  }

  @SuppressWarnings("unchecked")
  private <T> T getToAdd(String group, String name, Class<T> klass, Lock lock, Map<String, Map<String, T>> map) {
    boolean locked = false;
    try {
      Map<String, T> groupMap = map.get(group);
      if (groupMap == null) {
        lock.lock();
        locked = true;
        groupMap = map.get(group);
        if (groupMap == null) {
          groupMap = new ConcurrentHashMap<String, T>();
          map.put(group, groupMap);
        }
      }
      T element = groupMap.get(name);
      if (element == null) {
        if (!locked) {
          lock.lock();
          locked = true;
        }
        element = groupMap.get(name);
        if (element == null) {
          try {
            if (klass == Timer.class) {
              element = (T) new Timer(timersSize);
            } else {
              element = klass.newInstance();
            }
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
          groupMap.put(name, element);
        }
      }
      return element;
    } finally {
      if (locked) {
        lock.unlock();
      }
    }
  }

  static class Cron implements Instrumentation.Cron {
    long start;
    long lapStart;
    long own;
    long total;

    @Override
    public Cron start() {
      if (total != 0) {
        throw new IllegalStateException("Cron already used");
      }
      if (start == 0) {
        start = Time.now();
        lapStart = start;
      } else if (lapStart == 0) {
        lapStart = Time.now();
      }
      return this;
    }

    @Override
    public Cron stop() {
      if (total != 0) {
        throw new IllegalStateException("Cron already used");
      }
      if (lapStart > 0) {
        own += Time.now() - lapStart;
        lapStart = 0;
      }
      return this;
    }

    void end() {
      stop();
      total = Time.now() - start;
    }

  }

  static class Timer implements JSONAware, JSONStreamAware {
    static final int LAST_TOTAL = 0;
    static final int LAST_OWN = 1;
    static final int AVG_TOTAL = 2;
    static final int AVG_OWN = 3;

    Lock lock = new ReentrantLock();
    private long[] own;
    private long[] total;
    private int last;
    private boolean full;
    private int size;

    public Timer(int size) {
      this.size = size;
      own = new long[size];
      total = new long[size];
      for (int i = 0; i < size; i++) {
        own[i] = -1;
        total[i] = -1;
      }
      last = -1;
    }

    long[] getValues() {
      lock.lock();
      try {
        long[] values = new long[4];
        values[LAST_TOTAL] = total[last];
        values[LAST_OWN] = own[last];
        int limit = (full) ? size : (last + 1);
        for (int i = 0; i < limit; i++) {
          values[AVG_TOTAL] += total[i];
          values[AVG_OWN] += own[i];
        }
        values[AVG_TOTAL] = values[AVG_TOTAL] / limit;
        values[AVG_OWN] = values[AVG_OWN] / limit;
        return values;
      } finally {
        lock.unlock();
      }
    }

    void addCron(Cron cron) {
      cron.end();
      lock.lock();
      try {
        last = (last + 1) % size;
        full = full || last == (size - 1);
        total[last] = cron.total;
        own[last] = cron.own;
      } finally {
        lock.unlock();
      }
    }

    @SuppressWarnings("unchecked")
    private JSONObject getJSON() {
      long[] values = getValues();
      JSONObject json = new JSONObject();
      json.put("lastTotal", values[0]);
      json.put("lastOwn", values[1]);
      json.put("avgTotal", values[2]);
      json.put("avgOwn", values[3]);
      return json;
    }

    @Override
    public String toJSONString() {
      return getJSON().toJSONString();
    }

    @Override
    public void writeJSONString(Writer out) throws IOException {
      getJSON().writeJSONString(out);
    }

  }

  @Override
  public Cron createCron() {
    return new Cron();
  }

  @Override
  public void incr(String group, String name, long count) {
    AtomicLong counter = getToAdd(group, name, AtomicLong.class, counterLock, counters);
    counter.addAndGet(count);
  }

  @Override
  public void addCron(String group, String name, Instrumentation.Cron cron) {
    Timer timer = getToAdd(group, name, Timer.class, timerLock, timers);
    timer.addCron((Cron) cron);
  }

  static class VariableHolder<E> implements JSONAware, JSONStreamAware {
    Variable<E> var;

    public VariableHolder() {
    }

    public VariableHolder(Variable<E> var) {
      this.var = var;
    }

    @SuppressWarnings("unchecked")
    private JSONObject getJSON() {
      JSONObject json = new JSONObject();
      json.put("value", var.getValue());
      return json;
    }

    @Override
    public String toJSONString() {
      return getJSON().toJSONString();
    }

    @Override
    public void writeJSONString(Writer out) throws IOException {
      out.write(toJSONString());
    }

  }

  @Override
  public void addVariable(String group, String name, Variable<?> variable) {
    VariableHolder holder = getToAdd(group, name, VariableHolder.class, variableLock, variables);
    holder.var = variable;
  }

  static class Sampler implements JSONAware, JSONStreamAware {
    Variable<Long> variable;
    long[] values;
    private AtomicLong sum;
    private int last;
    private boolean full;

    void init(int size, Variable<Long> variable) {
      this.variable = variable;
      values = new long[size];
      sum = new AtomicLong();
      last = 0;
    }

    void sample() {
      int index = last;
      long valueGoingOut = values[last];
      full = full || last == (values.length - 1);
      last = (last + 1) % values.length;
      values[index] = variable.getValue();
      sum.addAndGet(-valueGoingOut + values[index]);
    }

    double getRate() {
      return ((double) sum.get()) / ((full) ? values.length : ((last == 0) ? 1 : last));
    }

    @SuppressWarnings("unchecked")
    private JSONObject getJSON() {
      JSONObject json = new JSONObject();
      json.put("sampler", getRate());
      json.put("size", (full) ? values.length : last);
      return json;
    }

    @Override
    public String toJSONString() {
      return getJSON().toJSONString();
    }

    @Override
    public void writeJSONString(Writer out) throws IOException {
      out.write(toJSONString());
    }
  }

  @Override
  public void addSampler(String group, String name, int samplingSize, Variable<Long> variable) {
    Sampler sampler = getToAdd(group, name, Sampler.class, samplerLock, samplers);
    samplerLock.lock();
    try {
      sampler.init(samplingSize, variable);
      samplersList.add(sampler);
    } finally {
      samplerLock.unlock();
    }
  }

  class SamplersRunnable implements Runnable {

    @Override
    public void run() {
      samplerLock.lock();
      try {
        for (Sampler sampler : samplersList) {
          sampler.sample();
        }
      } finally {
        samplerLock.unlock();
      }
    }
  }

  @Override
  public Map<String, Map<String, ?>> getSnapshot() {
    return all;
  }


}
