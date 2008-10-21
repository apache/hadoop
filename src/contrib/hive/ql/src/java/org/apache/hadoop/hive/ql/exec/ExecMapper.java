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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class ExecMapper extends MapReduceBase implements Mapper {

  private MapOperator mo;
  private OutputCollector oc;
  private JobConf jc;
  private boolean abort = false;
  private Reporter rp;
  public static final Log l4j = LogFactory.getLog("ExecMapper");
  private static boolean done;
  
  public void configure(JobConf job) {
    jc = job;
    mapredWork mrwork = Utilities.getMapRedWork(job);
    mo = new MapOperator ();
    mo.setConf(mrwork);
    // we don't initialize the operator until we have set the output collector
  }

  public void map(Object key, Object value,
                  OutputCollector output,
                  Reporter reporter) throws IOException {
    if(oc == null) {
      try {
        oc = output;
        mo.setOutputCollector(oc);
        mo.initialize(jc);
        rp = reporter;
      } catch (HiveException e) {
        abort = true;
        e.printStackTrace();
        throw new RuntimeException ("Map operator initialization failed", e);
      }
    }

    try {
      if (mo.getDone())
        done = true;
      else
        // Since there is no concept of a group, we don't invoke startGroup/endGroup for a mapper
        mo.process((Writable)value);
    } catch (HiveException e) {
      abort = true;
      e.printStackTrace();
      throw new RuntimeException (e.getMessage(), e);
    }
  }

  public void close() {
    // No row was processed
    if(oc == null) {
      try {
        l4j.trace("Close called no row");
        mo.initialize(jc);
        rp = null;
      } catch (HiveException e) {
        abort = true;
        e.printStackTrace();
        throw new RuntimeException ("Map operator close failed during initialize", e);
      }
    }

    // detecting failed executions by exceptions thrown by the operator tree
    // ideally hadoop should let us know whether map execution failed or not
    try {
      mo.close(abort);
      reportStats rps = new reportStats (rp);
      mo.preorderMap(rps);
      return;
    } catch (HiveException e) {
      if(!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException ("Error while closing operators");
      }
    }
  }

  public static boolean getDone() {
    return done;
  }

  public static class reportStats implements Operator.OperatorFunc {
    Reporter rp;
    public reportStats (Reporter rp) {
      this.rp = rp;
    }
    public void func(Operator op) {
      Map<Enum, Long> opStats = op.getStats();
      for(Map.Entry<Enum, Long> e: opStats.entrySet()) {
          if(this.rp != null) {
              rp.incrCounter(e.getKey(), e.getValue());
          }
      }
    }
  }
}
