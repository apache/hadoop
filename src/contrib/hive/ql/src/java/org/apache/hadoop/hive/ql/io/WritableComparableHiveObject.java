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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.ql.exec.HiveObject;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.mapredWork;

/**
 * A wrapper over hive objects that allows interfacing with Map-Reduce
 * sorting/serialization layers. 
 *
 * WritableComparable Hive Objects are deserialized both in map and reduce phases.
 * Because they have a fixed schema - we just need to initialize a single deserializer
 * Either the serializer/deserializer is passed in at construction time (map output)
 * or it is obtained via JobConf at construction time
 *
 * The base version allows a tag to be serialized out alongside. But it is not used for
 * grouping/partitioning (only for sorting).
 *
 */

public class WritableComparableHiveObject extends WritableHiveObject implements WritableComparable {

  /**
   * number of fields used in partition function.
   * 0 - no fields will be used
   * -1 - a random number will be used.
   * Integer.MAX_VALUE - use all key fields.
   */
  static int numPartitionFields = Integer.MAX_VALUE;
  static Random random = new Random();
  
  public static void setNumPartitionFields(int numPartitionFields) {
    WritableComparableHiveObject.numPartitionFields = numPartitionFields;
  }
  
  static List<SerDeField> topLevelFields = null;
  static boolean isPrimitive;
  static HiveObjectSerializer gHos;

  /**
   * Constructor called by Hive on map output
   */
  public WritableComparableHiveObject(int tag, HiveObject ho,  HiveObjectSerializer hos) {
    super(tag, ho, hos);
  }

  /**
   * Default constructor invoked when map-reduce is constructing this object
   */
  public WritableComparableHiveObject () {
    super();
  }

  /**
   * This function is invoked when map-reduce is constructing this object
   * We construct one global deserializer for this case.
   *
   * TODO: how to do this initialization without making this configurable?
   * Need to find a very early hook!
   * 
   * TODO: Replace NaiiveSerializer with MetadataTypedSerDe, and configure
   * the MetadataTypedSerDe right here.
   */
  @Override
  public void setConf(Configuration conf) {
    if(gHos == null) {
      mapredWork gWork = Utilities.getMapRedWork (conf);
      gHos = new NaiiveSerializer();
    }
    hos = gHos;
  }

  /**
   * Get tag out first like the base class - but use the same deserializer
   * for getting the hive object (since the key schema is constant)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    tag = (int) in.readByte();
    ho = hos.deserialize(in);
  }


  public boolean equals (Object o) {
    // afaik - this is never called
    throw new RuntimeException("Not invented here");
  }

  /**
   * This api should only be called during the reduce to check for group equality
   * This asserts default grouping behavior (group by all columns). Note we don't
   * use tags for grouping. Tags are only used for sorting and this behavior is
   * controlled by HiveObjectComparator
   * TODO (low priority): Make it possible to specify the grouping columns.
   */
  public int compareTo(Object o) {
    HiveObject ho_rhs = ((WritableComparableHiveObject)o).getHo();
    if(topLevelFields == null) {
      try {
        if(ho.isPrimitive()) {
          topLevelFields = HiveObject.nlist;
          isPrimitive = true;
        } else {
          topLevelFields = ho.getFields();
          isPrimitive = false;
        }
      } catch (HiveException e) {
        throw new RuntimeException ("Cannot get Fields from HiveObject");
      }
    }
    try {

      if(isPrimitive) {
        Comparable a = (Comparable)ho.getJavaObject();
        Comparable b = (Comparable)ho_rhs.getJavaObject();
        return  a.compareTo(b);
      }

      // there is an assumption that the number of fields are the same.
      // and that all the constituent fields are comparables.
      // Assumption #1 has to be enforced by the deserializer.
      // Assumption #2 has to be enforced by only allow primitive comparable types 
      // as group fields.
      for(SerDeField onef: topLevelFields) {
        Comparable a = (Comparable)ho.get(onef).getJavaObject();
        Comparable b = (Comparable)ho_rhs.get(onef).getJavaObject();
        
        int ret = a.compareTo(b);
        if(ret != 0)
          return (ret);
      }
    } catch (HiveException e) {
      e.printStackTrace();
      throw new RuntimeException ("HiveObject.get()/getJavaObject() methods failed");
    }

    // all fields are the same.
    return (0);
  }

  public int hashCode() {
    // This is a special case when we want the rows to be randomly distributed to  
    // reducers for load balancing problem.  In this case, we use a random number 
    // as the hashCode.
    if (numPartitionFields == -1) {
      return random.nextInt();
    }
    if(topLevelFields == null) {
      try {
        if(ho.isPrimitive()) {
          topLevelFields = HiveObject.nlist;
          isPrimitive = true;
        } else {
          topLevelFields = ho.getFields();
          isPrimitive = false;
        }
      } catch (HiveException e) {
        throw new RuntimeException ("Cannot get Fields from HiveObject");
      }
    }

    int ret = 0;
    try {
      if(isPrimitive) {
        return ho.getJavaObject().hashCode();
      }
      int numFields = 0;
      for(SerDeField onef: topLevelFields) {
        Object o = ho.get(onef).getJavaObject();
        // TODO: replace with something smarter (borrowed from Text.java)
        ret = ret * 31 + (o == null ? 0 : o.hashCode());
        numFields ++;
        if (numFields >= numPartitionFields) break;
      }
    } catch (HiveException e) {
      e.printStackTrace();
      throw new RuntimeException ("HiveObject.get()/getJavaObject() failed");
    }
    return (ret);
  }
}
