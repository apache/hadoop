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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.ql.exec.HiveObject;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.mapredWork;

/**
 * Extension of WritableComparableHiveObject that does not tag objects
 *
 */

public class NoTagWritableComparableHiveObject extends WritableComparableHiveObject implements WritableComparable {

  /**
   * Constructor called by Hive on map output
   */
  public NoTagWritableComparableHiveObject (HiveObject ho,  HiveObjectSerializer hos) {
    super(-1, ho, hos);
  }

  /**
   * Default constructor invoked when map-reduce is constructing this object
   */
  public NoTagWritableComparableHiveObject () {
    super();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ho = hos.deserialize(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    hos.serialize(ho, out);
  }

}
