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

package org.apache.hadoop.mapreduce;
 
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
 
/**
 * A factory to allow applications to deal with inconsistencies between
 * MapReduce Context Objects API between hadoop-0.20 and later versions.
 */
public class ContextFactory {

  private static final Constructor<?> JOB_CONTEXT_CONSTRUCTOR;
  private static final Constructor<?> TASK_CONTEXT_CONSTRUCTOR;
  private static final Constructor<?> MAP_CONTEXT_CONSTRUCTOR;
  private static final Constructor<?> MAP_CONTEXT_IMPL_CONSTRUCTOR;
  private static final boolean useV21;
  
  private static final Field REPORTER_FIELD;
  private static final Field READER_FIELD;
  private static final Field WRITER_FIELD;
  private static final Field OUTER_MAP_FIELD;
  private static final Field WRAPPED_CONTEXT_FIELD;
  
  static {
    boolean v21 = true;
    final String PACKAGE = "org.apache.hadoop.mapreduce";
    try {
      Class.forName(PACKAGE + ".task.JobContextImpl");
    } catch (ClassNotFoundException cnfe) {
      v21 = false;
    }
    useV21 = v21;
    Class<?> jobContextCls;
    Class<?> taskContextCls;
    Class<?> taskIOContextCls;
    Class<?> mapCls;
    Class<?> mapContextCls;
    Class<?> innerMapContextCls;
    try {
      if (v21) {
        jobContextCls = 
          Class.forName(PACKAGE+".task.JobContextImpl");
        taskContextCls = 
          Class.forName(PACKAGE+".task.TaskAttemptContextImpl");
        taskIOContextCls = 
          Class.forName(PACKAGE+".task.TaskInputOutputContextImpl");
        mapContextCls = Class.forName(PACKAGE + ".task.MapContextImpl");
        mapCls = Class.forName(PACKAGE + ".lib.map.WrappedMapper");
        innerMapContextCls = 
          Class.forName(PACKAGE+".lib.map.WrappedMapper$Context");
      } else {
        jobContextCls = 
          Class.forName(PACKAGE+".JobContext");
        taskContextCls = 
          Class.forName(PACKAGE+".TaskAttemptContext");
        taskIOContextCls = 
          Class.forName(PACKAGE+".TaskInputOutputContext");
        mapContextCls = Class.forName(PACKAGE + ".MapContext");
        mapCls = Class.forName(PACKAGE + ".Mapper");
        innerMapContextCls = 
          Class.forName(PACKAGE+".Mapper$Context");      
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class", e);
    }
    try {
      JOB_CONTEXT_CONSTRUCTOR = 
        jobContextCls.getConstructor(Configuration.class, JobID.class);
      JOB_CONTEXT_CONSTRUCTOR.setAccessible(true);
      TASK_CONTEXT_CONSTRUCTOR = 
        taskContextCls.getConstructor(Configuration.class, 
                                      TaskAttemptID.class);
      TASK_CONTEXT_CONSTRUCTOR.setAccessible(true);
      if (useV21) {
        MAP_CONTEXT_CONSTRUCTOR = 
          innerMapContextCls.getConstructor(mapCls,
                                            MapContext.class);
        MAP_CONTEXT_IMPL_CONSTRUCTOR =
          mapContextCls.getDeclaredConstructor(Configuration.class, 
                                               TaskAttemptID.class,
                                               RecordReader.class,
                                               RecordWriter.class,
                                               OutputCommitter.class,
                                               StatusReporter.class,
                                               InputSplit.class);
        MAP_CONTEXT_IMPL_CONSTRUCTOR.setAccessible(true);
        WRAPPED_CONTEXT_FIELD = 
          innerMapContextCls.getDeclaredField("mapContext");
        WRAPPED_CONTEXT_FIELD.setAccessible(true);
      } else {
        MAP_CONTEXT_CONSTRUCTOR = 
          innerMapContextCls.getConstructor(mapCls,
                                            Configuration.class, 
                                            TaskAttemptID.class,
                                            RecordReader.class,
                                            RecordWriter.class,
                                            OutputCommitter.class,
                                            StatusReporter.class,
                                            InputSplit.class);
        MAP_CONTEXT_IMPL_CONSTRUCTOR = null;
        WRAPPED_CONTEXT_FIELD = null;
      }
      MAP_CONTEXT_CONSTRUCTOR.setAccessible(true);
      REPORTER_FIELD = taskContextCls.getDeclaredField("reporter");
      REPORTER_FIELD.setAccessible(true);
      READER_FIELD = mapContextCls.getDeclaredField("reader");
      READER_FIELD.setAccessible(true);
      WRITER_FIELD = taskIOContextCls.getDeclaredField("output");
      WRITER_FIELD.setAccessible(true);
      OUTER_MAP_FIELD = innerMapContextCls.getDeclaredField("this$0");
      OUTER_MAP_FIELD.setAccessible(true);
    } catch (SecurityException e) {
      throw new IllegalArgumentException("Can't run constructor ", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Can't find constructor ", e);
    } catch (NoSuchFieldException e) {
      throw new IllegalArgumentException("Can't find field ", e);
    }
  }

  /**
   * Clone a {@link JobContext} or {@link TaskAttemptContext} with a 
   * new configuration.
   * @param original the original context
   * @param conf the new configuration
   * @return a new context object
   * @throws InterruptedException 
   * @throws IOException 
   */
  @SuppressWarnings("unchecked")
  public static JobContext cloneContext(JobContext original,
                                        Configuration conf
                                        ) throws IOException, 
                                                 InterruptedException {
    try {
      if (original instanceof MapContext<?,?,?,?>) {
        return cloneMapContext((Mapper.Context) original, conf, null, null);        
      } else if (original instanceof ReduceContext<?,?,?,?>) {
        throw new IllegalArgumentException("can't clone ReduceContext");
      } else if (original instanceof TaskAttemptContext) {
        TaskAttemptContext spec = (TaskAttemptContext) original;
        return (JobContext) 
          TASK_CONTEXT_CONSTRUCTOR.newInstance(conf, spec.getTaskAttemptID());
      } else {
        return (JobContext) 
        JOB_CONTEXT_CONSTRUCTOR.newInstance(conf, original.getJobID());           
      }
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't clone object", e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't clone object", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't clone object", e);
    }
  }
  
  /**
   * Copy a custom WrappedMapper.Context, optionally replacing 
   * the input and output.
   * @param <K1> input key type
   * @param <V1> input value type
   * @param <K2> output key type
   * @param <V2> output value type
   * @param context the context to clone
   * @param conf a new configuration
   * @param reader Reader to read from. Null means to clone from context.
   * @param writer Writer to write to. Null means to clone from context.
   * @return a new context. it will not be the same class as the original.
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public static <K1,V1,K2,V2> Mapper<K1,V1,K2,V2>.Context 
       cloneMapContext(MapContext<K1,V1,K2,V2> context,
                       Configuration conf,
                       RecordReader<K1,V1> reader,
                       RecordWriter<K2,V2> writer
                      ) throws IOException, InterruptedException {
    try {
      // get the outer object pointer
      Object outer = OUTER_MAP_FIELD.get(context);
      // if it is a wrapped 21 context, unwrap it
      if ("org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context".equals
            (context.getClass().getName())) {
        context = (MapContext<K1,V1,K2,V2>) WRAPPED_CONTEXT_FIELD.get(context);
      }
      // if the reader or writer aren't given, use the same ones
      if (reader == null) {
        reader = (RecordReader<K1,V1>) READER_FIELD.get(context);
      }
      if (writer == null) {
        writer = (RecordWriter<K2,V2>) WRITER_FIELD.get(context);
      }
      if (useV21) {
        Object basis = 
          MAP_CONTEXT_IMPL_CONSTRUCTOR.newInstance(conf, 
                                                   context.getTaskAttemptID(),
                                                   reader, writer,
                                                   context.getOutputCommitter(),
                                                   REPORTER_FIELD.get(context),
                                                   context.getInputSplit());
        return (Mapper.Context) 
          MAP_CONTEXT_CONSTRUCTOR.newInstance(outer, basis);
      } else {
        return (Mapper.Context)
          MAP_CONTEXT_CONSTRUCTOR.newInstance(outer,
                                              conf, context.getTaskAttemptID(),
                                              reader, writer,
                                              context.getOutputCommitter(),
                                              REPORTER_FIELD.get(context),
                                              context.getInputSplit());
      }
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't access field", e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't create object", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't invoke constructor", e);
    }
  }
}
