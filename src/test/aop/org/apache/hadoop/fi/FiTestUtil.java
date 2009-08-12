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
package org.apache.hadoop.fi;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Test Utilities */
public class FiTestUtil {
  /** Logging */
  public static final Log LOG = LogFactory.getLog(FiTestUtil.class);

  /** Return the method name of the callee. */
  public static String getMethodName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }

  /**
   * Sleep.
   * If there is an InterruptedException, re-throw it as a RuntimeException.
   */
  public static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /** Action interface */
  public static interface Action<T> {
    /** Run the action with the parameter. */
    public void run(T parameter) throws IOException;
  }

  /** An ActionContainer contains at most one action. */
  public static class ActionContainer<T> {
    private Action<T> action;

    /** Create an empty container. */
    public ActionContainer() {}

    /** Set action. */
    public void set(Action<T> a) {action = a;}

    /** Run the action if it exists. */
    public void run(T obj) throws IOException {
      if (action != null) {
        action.run(obj);
      }
    }
  }
}