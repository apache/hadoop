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
 * Writable Hive Objects are deserialized only in reduce phase. 'Value'
 * fields are encapsulated in WritableHiveObjects - and they are heterogenous.
 * Hence we use prefix a tag to the actual object to figure out different 
 * kinds of objects
 * 
 */

public class WritableHiveObject implements Writable, Configurable {
  protected int tag;
  protected HiveObject ho;
  protected HiveObjectSerializer hos;

  protected static HiveObjectSerializer [] mapredDeserializer;


  public WritableHiveObject () {}

  /**
   * Should not be called.
   */
  public Configuration getConf() {
    throw new RuntimeException ("Unexpected invocation");
  }

  /**
   * 
   * 1. we come through this code path when being initialized by map-reduce
   * 
   *    In this case we fall back on one common deserializer - but create an
   *    instance for each tag (since deserializers cannot deal with differing
   *    schemas
   *
   *    It turns out that objects may be deserialized and again serialized. This
   *    is done unnecessarily in some code paths in 0.15 - but is legitimate in 
   *    in the case of combiner.
   *
   * TODO: how to do this initialization without making this configurable? Need to 
   * find a very early hook!
   */
  public void setConf(Configuration conf) {
    if(mapredDeserializer == null) {
      mapredWork gWork = Utilities.getMapRedWork (conf);
      setSerialFormat();
    }
  }
  
  /**
   * Meant to be accessed directly from test code only 
   * 
   * TODO: this deserializers need to be initialized with the schema
   */
  public static void setSerialFormat() {
    mapredDeserializer = 
      new HiveObjectSerializer [(int) Byte.MAX_VALUE];
    
    for(int i=0; i<Byte.MAX_VALUE; i++) {
      // we initialize a deserializer for each tag. in future we will
      // pass the schema in the deserializer as well. For now ..
      // TODO: use MetadataTypedSerDe to replace NaviiveSerializer.
      mapredDeserializer[i] =  new NaiiveSerializer();
    }
  }


  /**
   * 2. when map-reduce initializes the object - we just read data.
   *    for each row we read the tag and then use the deserializer for
   *    that tag.
   *
   */
  public void readFields(DataInput in) throws IOException {
    tag = (int) in.readByte();
    // stash away the serializer in case we are written out
    hos = mapredDeserializer[tag];
    ho = hos.deserialize(in);
  }

  /**
   * 1. this constructor will be invoked by hive when creating writable  objects
   */
  public WritableHiveObject (int tag, HiveObject ho,  HiveObjectSerializer hos) {
    this.tag = tag;
    this.ho = ho;
    this.hos = hos;
  }

  /**
   * 2. when Hive instantiates Writable objects - we will repeatedly set a new object
   */
  public void setHo(HiveObject ho) {
    this.ho = ho;
  }

  /**
   * 3. and ask for the object to be serialized out
   */
  public void write(DataOutput out) throws IOException {
    out.write(tag);
    hos.serialize(ho, out);
  }


  public HiveObject getHo() {
    return (ho);
  }

  public int getTag() {
    return tag;
  }
}


