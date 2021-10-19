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

package org.apache.hadoop.yarn;

import java.util.Iterator;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;

/**
 * Utilities to generate fake test apps
 */
public class MockApps {
  static final Iterator<String> NAMES = Iterators.cycle("SleepJob",
      "RandomWriter", "TeraSort", "TeraGen", "PigLatin", "WordCount",
      "I18nApp<☯>");
  static final Iterator<String> USERS = Iterators.cycle("dorothy", "tinman",
      "scarecrow", "glinda", "nikko", "toto", "winkie", "zeke", "gulch");
  static final Iterator<YarnApplicationState> STATES = Iterators.cycle(
      YarnApplicationState.values());
  static final Iterator<String> QUEUES = Iterators.cycle("a.a1", "a.a2",
      "b.b1", "b.b2", "b.b3", "c.c1.c11", "c.c1.c12", "c.c1.c13",
      "c.c2", "c.c3", "c.c4");
  static final long TS = System.currentTimeMillis();

  public static String newAppName() {
    synchronized(NAMES) {
      return NAMES.next();
    }
  }

  public static String newUserName() {
    synchronized(USERS) {
      return USERS.next();
    }
  }

  public static String newQueue() {
    synchronized(QUEUES) {
      return QUEUES.next();
    }
  }

  public static ApplicationId newAppID(int i) {
    return ApplicationId.newInstance(TS, i);
  }

  public static YarnApplicationState newAppState() {
    synchronized(STATES) {
      return STATES.next();
    }
  }

}
