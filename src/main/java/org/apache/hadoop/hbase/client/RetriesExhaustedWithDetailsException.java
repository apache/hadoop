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
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Addressing;

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
@SuppressWarnings("serial")
public class RetriesExhaustedWithDetailsException
extends RetriesExhaustedException {
  List<Throwable> exceptions;
  List<Row> actions;
  List<String> hostnameAndPort;

  public RetriesExhaustedWithDetailsException(List<Throwable> exceptions,
                                              List<Row> actions,
                                              List<String> hostnameAndPort) {
    super("Failed " + exceptions.size() + " action" +
        pluralize(exceptions) + ": " +
        getDesc(exceptions, actions, hostnameAndPort));

    this.exceptions = exceptions;
    this.actions = actions;
    this.hostnameAndPort = hostnameAndPort;
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

  /**
   * @param i
   * @return
   * @deprecated
   */
  public HServerAddress getAddress(int i) {
    return new HServerAddress(Addressing.createInetSocketAddressFromHostAndPortStr(getHostnamePort(i)));
  }

  public String getHostnamePort(final int i) {
    return this.hostnameAndPort.get(i);
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
                               List<String> hostnamePort) {
    String s = getDesc(classifyExs(exceptions));
    s += "servers with issues: ";
    Set<String> uniqAddr = new HashSet<String>();
    uniqAddr.addAll(hostnamePort);
    for(String addr : uniqAddr) {
      s += addr + ", ";
    }
    return s;
  }

  public static Map<String, Integer> classifyExs(List<Throwable> ths) {
    Map<String, Integer> cls = new HashMap<String, Integer>();
    for (Throwable t : ths) {
      if (t == null) continue;
      String name = "";
      if (t instanceof NoSuchColumnFamilyException) {
        name = t.getMessage();
      } else {
        name = t.getClass().getSimpleName();
      }
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
