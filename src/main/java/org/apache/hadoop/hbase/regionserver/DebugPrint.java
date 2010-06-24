/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DebugPrint {

private static final AtomicBoolean enabled = new AtomicBoolean(false);
  private static final Object sync = new Object();
  public static StringBuilder out = new StringBuilder();

  static public void enable() {
    enabled.set(true);
  }
  static public void disable() {
    enabled.set(false);
  }

  static public void reset() {
    synchronized (sync) {
      enable(); // someone wants us enabled basically.

      out = new StringBuilder();
    }
  }
  static public void dumpToFile(String file) throws IOException {
    FileWriter f = new FileWriter(file);
    synchronized (sync) {
      f.write(out.toString());
    }
    f.close();
  }

  public static void println(String m) {
    if (!enabled.get()) {
      System.out.println(m);
      return;
    }

    synchronized (sync) {
      String threadName = Thread.currentThread().getName();
      out.append("<");
      out.append(threadName);
      out.append("> ");
      out.append(m);
      out.append("\n");
    }
  }
}
