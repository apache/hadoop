/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.lang.reflect.Constructor;
import java.io.*;
import java.lang.management.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;

/**
 * General reflection utils
 */

public class ReflectionUtils {
    
    private static final Class[] emptyArray = new Class[]{};

    /** Create an object for the given class and initialize it from conf
     * 
     * @param theClass class of which an object is created
     * @param conf Configuration
     * @return a new object
     */
    public static Object newInstance(Class theClass, Configuration conf) {
        Object result;
        try {
            Constructor meth = theClass.getDeclaredConstructor(emptyArray);
            meth.setAccessible(true);
            result = meth.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (conf != null) {
            if (result instanceof Configurable) {
                ((Configurable) result).setConf(conf);
            }
            if (conf instanceof JobConf && 
                    result instanceof JobConfigurable) {
                ((JobConfigurable)result).configure((JobConf) conf);
            }
        }
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
     * Print all of the thread's information and stack traces
     * @author Owen O'Malley
     * @param stream the stream to
     * @param title a string title for the stack trace
     */
    public static void printThreadInfo(PrintWriter stream,
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
     */
    public static synchronized void logThreadInfo(Log log,
                                                  String title,
                                                  long minInterval) {
      if (log.isInfoEnabled()) {
        long now = System.currentTimeMillis();
        if (now - previousLogTime >= minInterval * 1000) {
          previousLogTime = now;
          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          printThreadInfo(new PrintWriter(buffer), title);
          log.info(buffer.toString());
        }
      }
    }
}
