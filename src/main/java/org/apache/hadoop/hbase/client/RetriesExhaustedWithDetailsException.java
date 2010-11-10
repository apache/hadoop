/*
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HServerAddress;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This subclass of {@link org.apache.hadoop.hbase.client.RetriesExhaustedException}
 * is thrown when we have more information about which rows were causing which
 * exceptions on what servers.  You can call {@link #mayHaveClusterIssues()}
 * and if the result is false, you have input error problems, otherwise you
 * may have cluster issues.  You can iterate over the causes, rows and last
 * known server addresses via {@link #getNumExceptions()} and
 * {@link #getCause(int)}, {@link #getRow(int)} and {@link #getAddress(int)}.
 */
public class RetriesExhaustedWithDetailsException extends RetriesExhaustedException {

  List<Throwable> exceptions;
  List<Row> actions;
  List<HServerAddress> addresses;

  public RetriesExhaustedWithDetailsException(List<Throwable> exceptions,
                                              List<Row> actions,
                                              List<HServerAddress> addresses) {
    super("Failed " + exceptions.size() + " action" +
        pluralize(exceptions) + ": " +
        getDesc(exceptions,actions,addresses));

    this.exceptions = exceptions;
    this.actions = actions;
    this.addresses = addresses;
  }

  public List<Throwable> getCauses() {
    return exceptions;
  }

  public int getNumExceptions() {
    return exceptions.size();
  }

  public Throwable getCause(int i) {
    return exceptions.get(i);
  }

  public Row getRow(int i) {
    return actions.get(i);
  }

  public HServerAddress getAddress(int i) {
    return addresses.get(i);
  }

  public boolean mayHaveClusterIssues() {
    boolean res = false;

    // If all of the exceptions are DNRIOE not exception
    for (Throwable t : exceptions) {
      if ( !(t instanceof DoNotRetryIOException)) {
        res = true;
      }
    }
    return res;
  }


  public static String pluralize(Collection<?> c) {
    return pluralize(c.size());
  }

  public static String pluralize(int c) {
    return c > 1 ? "s" : "";
  }

  public static String getDesc(List<Throwable> exceptions,
                               List<Row> actions,
                               List<HServerAddress> addresses) {
    String s = getDesc(classifyExs(exceptions));
    s += "servers with issues: ";
    Set<HServerAddress> uniqAddr = new HashSet<HServerAddress>();
    uniqAddr.addAll(addresses);
    for(HServerAddress addr : uniqAddr) {
      s += addr + ", ";
    }
    return s;
  }

  public static Map<String, Integer> classifyExs(List<Throwable> ths) {
    Map<String, Integer> cls = new HashMap<String, Integer>();
    for (Throwable t : ths) {
      if (t == null) continue;
      String name = t.getClass().getSimpleName();
      Integer i = cls.get(name);
      if (i == null) {
        i = 0;
      }
      i += 1;
      cls.put(name, i);
    }
    return cls;
  }

  public static String getDesc(Map<String,Integer> classificaton) {
    String s = "";
    for (Map.Entry<String, Integer> e : classificaton.entrySet()) {
      s += e.getKey() + ": " + e.getValue() + " time" +
          pluralize(e.getValue()) + ", ";
    }
    return s;
  }

}
