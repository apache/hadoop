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

package org.apache.hadoop.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.slf4j.Logger;

/**
 * General reflection utils
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ReflectionUtils {
    
  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
  volatile private static SerializationFactory serialFactory = null;

  /** 
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = 
    new ConcurrentHashMap<Class<?>, Constructor<?>>();

  private static Class reentrantReadWriteLockSync = null;
  private static Class reentrantLockSync = null;
  private static Class abstractQueuedSynchronizerClass = null;

  static {
    try {
      reentrantReadWriteLockSync =
          Class.forName(
              "java.util.concurrent.locks.ReentrantReadWriteLock$Sync");
      reentrantLockSync =
          Class.forName("java.util.concurrent.locks.ReentrantLock$Sync");
      abstractQueuedSynchronizerClass =
          Class.forName(
              "java.util.concurrent.locks.AbstractQueuedSynchronizer");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  /**
   * Check and set 'configuration' if necessary.
   * 
   * @param theObject object for which to set configuration
   * @param conf Configuration
   */
  public static void setConf(Object theObject, Configuration conf) {
    if (conf != null) {
      if (theObject instanceof Configurable) {
        ((Configurable) theObject).setConf(conf);
      }
      setJobConf(theObject, conf);
    }
  }
  
  /**
   * This code is to support backward compatibility and break the compile  
   * time dependency of core on mapred.
   * This should be made deprecated along with the mapred package HADOOP-1230. 
   * Should be removed when mapred package is removed.
   */
  private static void setJobConf(Object theObject, Configuration conf) {
    //If JobConf and JobConfigurable are in classpath, AND
    //theObject is of type JobConfigurable AND
    //conf is of type JobConf then
    //invoke configure on theObject
    try {
      Class<?> jobConfClass = 
        conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConf");
      if (jobConfClass == null) {
        return;
      }
      
      Class<?> jobConfigurableClass = 
        conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConfigurable");
      if (jobConfigurableClass == null) {
        return;
      }
      if (jobConfClass.isAssignableFrom(conf.getClass()) &&
            jobConfigurableClass.isAssignableFrom(theObject.getClass())) {
        Method configureMethod = 
          jobConfigurableClass.getMethod("configure", jobConfClass);
        configureMethod.invoke(theObject, conf);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error in configuring object", e);
    }
  }

  /** Create an object for the given class and initialize it from conf
   * 
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @param <T> Generics Type T.
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass, Configuration conf) {
    return newInstance(theClass, conf, EMPTY_ARRAY);
  }

  /** Create an object for the given class and initialize it from conf
   *
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @param argTypes the types of the arguments
   * @param values the values of the arguments
   * @param <T> Generics Type.
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass, Configuration conf,
      Class<?>[] argTypes, Object ... values) {
    T result;
    if (argTypes.length != values.length) {
      throw new IllegalArgumentException(argTypes.length
          + " parameters are required but "
          + values.length
          + " arguments are provided");
    }
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(argTypes);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(values);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    setConf(result, conf);
    return result;
  }

  static private ThreadMXBean threadBean = 
    ManagementFactory.getThreadMXBean();
    
  public static void setContentionTracing(boolean val) {
    threadBean.setThreadContentionMonitoringEnabled(val);
  }
    
  private static String getTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }
    
  /**
   * Print all of the thread's information and stack traces.
   * 
   * @param stream the stream to
   * @param title a string title for the stack trace
   */
  public synchronized static void printThreadInfo(PrintStream stream,
                                     String title) {
    final int STACK_DEPTH = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: " + title);
    stream.println(threadIds.length + " active threads");
    for (long tid: threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread " + 
                     getTaskName(info.getThreadId(),
                                 info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      stream.println("  State: " + state);
      stream.println("  Blocked count: " + info.getBlockedCount());
      stream.println("  Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime());
        stream.println("  Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else  if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName());
        stream.println("  Blocked by " + 
                       getTaskName(info.getLockOwnerId(),
                                   info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (StackTraceElement frame: info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
    }
    stream.flush();
  }
    
  private static long previousLogTime = 0;

  /**
   * Log the current thread stacks at INFO level.
   * @param log the logger that logs the stack trace
   * @param title a descriptive title for the call stacks
   * @param minInterval the minimum time from the last
   * @deprecated to be removed with 3.4.0. Use {@link #logThreadInfo(Logger, String, long)} instead.
   */
  @Deprecated
  public static void logThreadInfo(org.apache.commons.logging.Log log,
      String title,
      long minInterval) {
    boolean dumpStack = false;
    if (log.isInfoEnabled()) {
      synchronized (ReflectionUtils.class) {
        long now = Time.monotonicNow();
        if (now - previousLogTime >= minInterval * 1000) {
          previousLogTime = now;
          dumpStack = true;
        }
      }
      if (dumpStack) {
        try {
          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          printThreadInfo(new PrintStream(buffer, false, "UTF-8"), title);
          log.info(buffer.toString(StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException ignored) {
        }
      }
    }
  }

  /**
   * Log the current thread stacks at INFO level.
   * @param log the logger that logs the stack trace
   * @param title a descriptive title for the call stacks
   * @param minInterval the minimum time from the last
   */
  public static void logThreadInfo(Logger log,
                                   String title,
                                   long minInterval) {
    boolean dumpStack = false;
    if (log.isInfoEnabled()) {
      synchronized (ReflectionUtils.class) {
        long now = Time.monotonicNow();
        if (now - previousLogTime >= minInterval * 1000) {
          previousLogTime = now;
          dumpStack = true;
        }
      }
      if (dumpStack) {
        try {
          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          printThreadInfo(new PrintStream(buffer, false, "UTF-8"), title);
          log.info(buffer.toString(StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException ignored) {
        }
      }
    }
  }

  /**
   * Return the correctly-typed {@link Class} of the given object.
   *  
   * @param o object whose correctly-typed <code>Class</code> is to be obtained
   * @param <T> Generics Type T.
   * @return the correctly typed <code>Class</code> of the given object.
   */
  @SuppressWarnings("unchecked")
  public static <T> Class<T> getClass(T o) {
    return (Class<T>)o.getClass();
  }
  
  // methods to support testing
  static void clearCache() {
    CONSTRUCTOR_CACHE.clear();
  }
    
  static int getCacheSize() {
    return CONSTRUCTOR_CACHE.size();
  }
  /**
   * A pair of input/output buffers that we use to clone writables.
   */
  private static class CopyInCopyOutBuffer {
    DataOutputBuffer outBuffer = new DataOutputBuffer();
    DataInputBuffer inBuffer = new DataInputBuffer();
    /**
     * Move the data from the output buffer to the input buffer.
     */
    void moveData() {
      inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
    }
  }
  
  /**
   * Allocate a buffer for each thread that tries to clone objects.
   */
  private static final ThreadLocal<CopyInCopyOutBuffer> CLONE_BUFFERS
      = new ThreadLocal<CopyInCopyOutBuffer>() {
      @Override
      protected synchronized CopyInCopyOutBuffer initialValue() {
        return new CopyInCopyOutBuffer();
      }
    };

  private static SerializationFactory getFactory(Configuration conf) {
    if (serialFactory == null) {
      serialFactory = new SerializationFactory(conf);
    }
    return serialFactory;
  }
  
  /**
   * Make a copy of the writable object using serialization to a buffer.
   * @param src the object to copy from
   * @param dst the object to copy into, which is destroyed
   * @param <T> Generics Type.
   * @param conf configuration.
   * @return dst param (the copy)
   * @throws IOException raised on errors performing I/O.
   */
  @SuppressWarnings("unchecked")
  public static <T> T copy(Configuration conf, 
                                T src, T dst) throws IOException {
    CopyInCopyOutBuffer buffer = CLONE_BUFFERS.get();
    buffer.outBuffer.reset();
    SerializationFactory factory = getFactory(conf);
    Class<T> cls = (Class<T>) src.getClass();
    Serializer<T> serializer = factory.getSerializer(cls);
    serializer.open(buffer.outBuffer);
    serializer.serialize(src);
    buffer.moveData();
    Deserializer<T> deserializer = factory.getDeserializer(cls);
    deserializer.open(buffer.inBuffer);
    dst = deserializer.deserialize(dst);
    return dst;
  }

  @Deprecated
  public static void cloneWritableInto(Writable dst, 
                                       Writable src) throws IOException {
    CopyInCopyOutBuffer buffer = CLONE_BUFFERS.get();
    buffer.outBuffer.reset();
    src.write(buffer.outBuffer);
    buffer.moveData();
    dst.readFields(buffer.inBuffer);
  }
  
  /**
   * Gets all the declared fields of a class including fields declared in
   * superclasses.
   *
   * @param clazz clazz
   * @return field List
   */
  public static List<Field> getDeclaredFieldsIncludingInherited(Class<?> clazz) {
    List<Field> fields = new ArrayList<Field>();
    while (clazz != null) {
      Field[] sortedFields = clazz.getDeclaredFields();
      Arrays.sort(sortedFields, new Comparator<Field>() {
        public int compare(Field a, Field b) {
          return a.getName().compareTo(b.getName());
        }
      });
      for (Field field : sortedFields) {
        fields.add(field);
      }
      clazz = clazz.getSuperclass();
    }
    
    return fields;
  }
  
  /**
   * Gets all the declared methods of a class including methods declared in
   * superclasses.
   *
   * @param clazz clazz.
   * @return Method List.
   */
  public static List<Method> getDeclaredMethodsIncludingInherited(Class<?> clazz) {
    List<Method> methods = new ArrayList<Method>();
    while (clazz != null) {
      for (Method method : clazz.getDeclaredMethods()) {
        methods.add(method);
      }
      clazz = clazz.getSuperclass();
    }
    
    return methods;
  }

  public static Object getDeclaredFieldValue(Class cls, String fieldName,
      Object obj) {
    try {
      Field field = cls.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(obj);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dumpThread(PrintStream out, Thread thread) {
    out.println("    thread: " + thread
        + ", tid=" + thread.getId() + ", state=" + thread.getState());
    out.println("    Stack:");
    for (StackTraceElement frame : thread.getStackTrace()) {
      out.println("      " + frame.toString());
    }
  }

  public static Object callDeclaredMethod(Class cls, String methodName,
      Object obj) {
    try {
      Method method = cls.getDeclaredMethod(methodName);
      method.setAccessible(true);
      return method.invoke(obj);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dumpLockInfo(Object sync,
      PrintStream out,
      boolean dumpWaiter,
      boolean dumpParkBlocker) {
    out.println("  parkBlocker : " + sync);
    Class cls;
    if (reentrantLockSync != null && reentrantLockSync.isInstance(sync)) {
      cls = reentrantLockSync;
    } else if (reentrantReadWriteLockSync != null &&
        reentrantReadWriteLockSync.isInstance(sync)) {
      cls = reentrantReadWriteLockSync;
    } else {
      out.println("  i don't know the " + sync.getClass() + " : " + sync);
      return;
    }
    Thread owner = (Thread) callDeclaredMethod(cls, "getOwner", sync);
    if (owner != null) {
      out.println("  owner:");
      dumpThread(out, owner);
    }

    if (reentrantReadWriteLockSync != null &&
        reentrantReadWriteLockSync.isInstance(sync)) {
      Thread firstReader =
          (Thread) getDeclaredFieldValue(cls, "firstReader", sync);
      if (firstReader != null) {
        out.println("  first reader:");
        dumpThread(out, firstReader);
      }
    }

    if (dumpWaiter) {
      out.println("  waiter threads:");
      Object head =
          getDeclaredFieldValue(abstractQueuedSynchronizerClass, "head", sync);
      if (head != null) {
        Object next = getDeclaredFieldValue(head.getClass(), "next", head);

        int index = 0;
        while (next != null) {
          Thread thread =
              (Thread) getDeclaredFieldValue(next.getClass(), "thread", next);
          out.println("  waiter " + index + ":");
          next = getDeclaredFieldValue(head.getClass(), "next", next);
          index++;
          if (thread == null) {
            continue;
          }
          dumpThread(out, thread);
          Thread.State state = thread.getState();
          Object parkBlocker =
              getDeclaredFieldValue(Thread.class, "parkBlocker", thread);
          if ((state == Thread.State.WAITING || state == Thread.State.BLOCKED)
              && parkBlocker != null
              && dumpParkBlocker) {
            out.println("+++++++++++++lock info++++++++++++++++");
            dumpLockInfo(parkBlocker, out, false, false);
            out.println("-------------lock info----------------");
          }
        }
      }
    }
  }

  /**
   * Print all of the thread's information and stack traces.
   *
   * @param stream the stream to
   * @param title a string title for the stack trace
   * @param stackDepth the depth of thread call stack
   */
  public synchronized static void printThreadInfoExt(PrintStream stream,
      String title,
      int stackDepth) {
    if (stackDepth <= 0) {
      stackDepth = 20;
    }
    Thread[] threads =
        (Thread[]) callDeclaredMethod(Thread.class, "getThreads", null);
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: " + title);
    stream.println(threadIds.length + " active threads");
    for (Thread thread : threads) {
      long tid = thread.getId();
      ThreadInfo info = threadBean.getThreadInfo(tid, stackDepth);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread " +
          getTaskName(info.getThreadId(),
              info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      stream.println("  State: " + state);
      stream.println("  Blocked count: " + info.getBlockedCount());
      stream.println("  Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime());
        stream.println("  Waited time: " + info.getWaitedTime());
      }
      Object parkBlocker = getDeclaredFieldValue(Thread.class,
          "parkBlocker", thread);
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName());
        stream.println("  Blocked by " +
            getTaskName(info.getLockOwnerId(),
                info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (StackTraceElement frame: info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
      if ((state == Thread.State.WAITING || state == Thread.State.BLOCKED)
          && parkBlocker != null) {
        stream.println("+++++++++++++++++++++++++++++");
        dumpLockInfo(parkBlocker, stream, false, false);
        stream.println("-----------------------------");
      }
    }
    stream.flush();
  }
}
