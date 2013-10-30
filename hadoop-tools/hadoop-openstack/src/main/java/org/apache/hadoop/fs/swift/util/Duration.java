/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.util;

public class Duration {

  private final long started;
  private long finished;

  public Duration() {
    started = time();
    finished = started;
  }

  private long time() {
    return System.currentTimeMillis();
  }

  public void finished() {
    finished = time();
  }

  public String getDurationString() {
    return humanTime(value());
  }

  public static String humanTime(long time) {
    long seconds = (time / 1000);
    long minutes = (seconds / 60);
    return String.format("%d:%02d:%03d", minutes, seconds % 60, time % 1000);
  }

  @Override
  public String toString() {
    return getDurationString();
  }

  public long value() {
    return finished -started;
  }
}
