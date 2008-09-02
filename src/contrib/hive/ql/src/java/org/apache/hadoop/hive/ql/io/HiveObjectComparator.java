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

package org.apache.hadoop.hive.ql.io;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.ql.exec.HiveObject;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.mapredWork;

/** 
 * Comparator for WritableComparableHiveObjects
 * 
 * We pass this in explicitly as a output key comparator so that we get a chance to
 * initialize the comparator using the job configuration/Hive plan. This allows us
 * to pick up the right deserializer (if we register with WritableComparator - we get
 * no such shot).
 *
 * This class provides a sort implementation only. The grouping implementation uses
 * the base compare() implementation - which just invokes compareTo() on the underlying
 * WritableComparableHiveObject.
 *
 */ 

public class HiveObjectComparator extends WritableComparator implements Configurable {

  // the serializer used to compare hive objects
  protected HiveObjectSerializer hos;
  protected boolean isTagged;

  public Configuration getConf() {
    throw new RuntimeException ("Unexpected invocation");
  }

  public void setConf(Configuration conf) {
    mapredWork gWork = Utilities.getMapRedWork (conf);
    isTagged = gWork.getNeedsTagging(); 
    hos = new NaiiveSerializer();
  }


  public HiveObjectComparator(Class c) {
    super(c);
  }


  public HiveObjectComparator () {
    this(WritableComparableHiveObject.class);
  }

  /**
   * This is the interface used to sort WritableComparableHiveObjects
   * If the objects are not tagged - then it's simple - we just sort them (for now)
   * based on the serialized object. If it is tagged - then we need to use the
   * serialized object as the higher order bits to sort on (so that grouping is
   * maintained) and then use the tag to break the tie (so that things are ordered
   * by tag in the same co-group
   */
  public int compare(byte[] b1, int s1, int l1,
                     byte[] b2, int s2, int l2) {
    if(!isTagged) {
      return (hos.compare(b1, s1, l1, b2, s2, l2));
    } else {
      int ret = hos.compare(b1, s1+1, l1-1, b2, s2+1, l2-1);
      if(ret == 0) {
        // use tag to break tie
        ret = ((int)(b1[s1] & 0xff)) - ((int)(b2[s2] & 0xff));
      }
      return (ret);
    }
  }
}
