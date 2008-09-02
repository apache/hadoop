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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.ql.exec.HiveObject;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.mapredWork;


/**
 * A wrapper over hive objects that allows interfacing with Map-Reduce
 * serialization layer.
 *
 * NoTag Writable Hive Objects are deserialized only in reduce phase. They are
 * used only when the 'value' fields in the reduce phase are homogenous and don't 
 * require tagging
 * 
 */

public class NoTagWritableHiveObject extends WritableHiveObject implements Writable, Configurable {

  public NoTagWritableHiveObject () { super(); }

  /**
   * This constructor will be invoked by hive when creating writable  objects.
   */
  public NoTagWritableHiveObject (int tag, HiveObject ho,  HiveObjectSerializer hos) {
    throw new RuntimeException ("NoTagWritables should not be initialized with tags");
  }

  public NoTagWritableHiveObject (HiveObject ho,  HiveObjectSerializer hos) {
    super(-1, ho, hos);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    // we need just one deserializer. Get the first of them!
    hos = mapredDeserializer[0];
  }

  @Override
  public int getTag() {
    return -1;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // don't serialize tag
    hos.serialize(ho, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // don't de-serialize tag
    ho = hos.deserialize(in);
  }
}
