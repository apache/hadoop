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

package org.apache.hadoop.mapreduce.lib.map;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Multithreaded implementation for @link org.apache.hadoop.mapreduce.Mapper.
 * <p>
 * It can be used instead of the default implementation,
 * @link org.apache.hadoop.mapred.MapRunner, when the Map operation is not CPU
 * bound in order to improve throughput.
 * <p>
 * Mapper implementations using this MapRunnable must be thread-safe.
 * <p>
 * The Map-Reduce job has to be configured with the mapper to use via 
 * {@link #setMapperClass(Configuration, Class)} and
 * the number of thread the thread-pool can use with the
 * {@link #getNumberOfThreads(Configuration) method. The default
 * value is 10 threads.
 * <p>
 */
public class MultithreadedMapper<K1, V1, K2, V2> 
  extends Mapper<K1, V1, K2, V2> {

  private static final Log LOG = LogFactory.getLog(MultithreadedMapper.class);
  private Class<Mapper<K1,V1,K2,V2>> mapClass;
  private Context outer;
  private MapRunner[] runners;

  public static int getNumberOfThreads(Configuration conf) {
    return conf.getInt("mapred.map.multithreadedrunner.threads", 10);
  }

  public static void setNumberOfThreads(Configuration conf, int threads) {
    conf.setInt("mapred.map.multithreadedrunner.threads", threads);
  }

  @SuppressWarnings("unchecked")
  public static <K1,V1,K2,V2>
  Class<Mapper<K1,V1,K2,V2>> getMapperClass(Configuration conf) {
    return (Class<Mapper<K1,V1,K2,V2>>) 
           conf.getClass("mapred.map.multithreadedrunner.class",
                         Mapper.class);
  }
  
  public static <K1,V1,K2,V2> 
  void setMapperClass(Configuration conf, 
                      Class<Mapper<K1,V1,K2,V2>> cls) {
    if (MultithreadedMapper.class.isAssignableFrom(cls)) {
      throw new IllegalArgumentException("Can't have recursive " + 
                                         "MultithreadedMapper instances.");
    }
    conf.setClass("mapred.map.multithreadedrunner.class", cls, Mapper.class);
  }

  public void run(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    outer = context;
    int numberOfThreads = getNumberOfThreads(conf);
    mapClass = getMapperClass(conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configuring multithread runner to use " + numberOfThreads + 
                " threads");
    }
    
    runners = (MapRunner[]) new Object[numberOfThreads];
    for(int i=0; i < numberOfThreads; ++i) {
      runners[i] = new MapRunner();
      runners[i].start();
    }
    for(int i=0; i < numberOfThreads; ++i) {
      runners[i].join();
      Throwable th = runners[i].throwable;
      if (th != null) {
        if (th instanceof IOException) {
          throw (IOException) th;
        } else if (th instanceof InterruptedException) {
          throw (InterruptedException) th;
        } else {
          throw (RuntimeException) th;
        }
      }
    }
  }

  private class SubMapContext extends Context {
    private K1 key;
    private V1 value;
    
    SubMapContext() {
      super(outer.getConfiguration(), outer.getTaskAttemptId());
    }

    @Override
    public InputSplit getInputSplit() {
      synchronized (outer) {
        return outer.getInputSplit();
      }
    }

    @Override
    public Counter getCounter(Enum<?> counterName) {
      synchronized (outer) {
        return outer.getCounter(counterName);
      }
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
      synchronized (outer) {
        return outer.getCounter(groupName, counterName);
      }
    }

    @Override
    public void progress() {
      synchronized (outer) {
        outer.progress();
      }
    }

    @Override
    public void collect(K2 key, V2 value) throws IOException,
                                         InterruptedException {
      synchronized (outer) {
        outer.collect(key, value);
      }
    }

    @Override
    public K1 nextKey(K1 k) throws IOException, InterruptedException {
      synchronized (outer) {
        key = outer.nextKey(key);
        if (key != null) {
          value = outer.nextValue(value);
        }
        return key;
      }
    }
    
    public V1 nextValue(V1 v) throws IOException, InterruptedException {
      return value;
    }
  }

  private class MapRunner extends Thread {
    private Mapper<K1,V1,K2,V2> mapper;
    private Context context;
    private Throwable throwable;

    @SuppressWarnings("unchecked")
    MapRunner() {
      mapper = (Mapper<K1,V1,K2,V2>) 
        ReflectionUtils.newInstance(mapClass, context.getConfiguration());
      context = new SubMapContext();
    }

    public Throwable getThrowable() {
      return throwable;
    }

    public void run() {
      try {
        mapper.run(context);
      } catch (Throwable ie) {
        throwable = ie;
      }
    }
  }

}
