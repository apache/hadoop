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

import org.apache.hadoop.mapred.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.WritableHiveObject;
import org.apache.hadoop.hive.ql.io.WritableComparableHiveObject;


import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.ExecMapper.reportStats;

public class ExecReducer extends MapReduceBase implements Reducer {

  private JobConf jc;
  private OutputCollector oc;
  private Operator reducer;
  private Reporter rp;
  private boolean abort = false;
  private boolean isTagged = false;
  private final HiveObject [] tagObjects =  new HiveObject [Byte.MAX_VALUE];

  private static String [] fieldNames;
  public static final Log l4j = LogFactory.getLog("ExecReducer");

  static {
    ArrayList<String> fieldNameArray =  new ArrayList<String> ();
    for(Utilities.ReduceField r: Utilities.ReduceField.values()) {
      fieldNameArray.add(r.toString());
    }
    fieldNames = fieldNameArray.toArray(new String [0]);
  }


  public void configure(JobConf job) {
    jc = job;
    mapredWork gWork = Utilities.getMapRedWork(job);
    reducer = gWork.getReducer();
    reducer.setMapredWork(gWork);
    isTagged = gWork.getNeedsTagging();

    // create a hive object to encapsulate each one of the potential tags      
    for(int i=0; i<Byte.MAX_VALUE; i++) {
      tagObjects[i] = new PrimitiveHiveObject(Byte.valueOf((byte)i));
    }
  }

  public void reduce(Object key, Iterator values,
                     OutputCollector output,
                     Reporter reporter) throws IOException {

    if(oc == null) {
      try {
        oc = output;
        reducer.setOutputCollector(oc);
        reducer.initialize(jc);
        rp = reporter;
      } catch (HiveException e) {
        abort = true;
        e.printStackTrace();
        throw new RuntimeException ("Reduce operator initialization failed");
      }
    }

    try {
      // the key is either a WritableComparable or a NoTagWritableComparable
      HiveObject keyObject = ((WritableComparableHiveObject)key).getHo();
      //System.err.print(keyObject.toString());
      // If a operator wants to do some work at the beginning of a group
      reducer.startGroup();
      while(values.hasNext()) {
        WritableHiveObject who = (WritableHiveObject)values.next();
       //System.err.print(who.getHo().toString());

        LabeledCompositeHiveObject lho = new LabeledCompositeHiveObject(fieldNames);
        lho.addHiveObject(keyObject);
        lho.addHiveObject(who.getHo());
        if(isTagged) {
          lho.addHiveObject(tagObjects[who.getTag()]);
        }
        reducer.process(lho);
      }

      // If a operator wants to do some work at the end of a group
      reducer.endGroup();

    } catch (HiveException e) {
      abort = true;
      throw new IOException (e.getMessage());
    }
  }

  public void close() {
    try {
      reducer.close(abort);
      reportStats rps = new reportStats (rp);
      reducer.preorderMap(rps);
      return;
    } catch (HiveException e) {
      if(!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException ("Error while closing operators: " + e.getMessage());
      }
    }
  }
}
