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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.ExecMapper.reportStats;
import org.apache.hadoop.hive.serde2.ColumnSet;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.MetadataListStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class ExecReducer extends MapReduceBase implements Reducer {

  private JobConf jc;
  private OutputCollector<?,?> oc;
  private Operator<?> reducer;
  private Reporter rp;
  private boolean abort = false;
  private boolean isTagged = false;

  private static String [] fieldNames;
  public static final Log l4j = LogFactory.getLog("ExecReducer");

  // TODO: move to DynamicSerDe when it's ready
  private Deserializer inputKeyDeserializer;
  // Input value serde needs to be an array to support different SerDe 
  // for different tags
  private SerDe[] inputValueDeserializer = new SerDe[Byte.MAX_VALUE];
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
    try {
      // We should initialize the SerDe with the TypeInfo when available.
      tableDesc keyTableDesc = PlanUtils.getReduceKeyDesc(gWork);
      inputKeyDeserializer = (SerDe)ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(), null);
      inputKeyDeserializer.initialize(null, keyTableDesc.getProperties());
      for(int tag=0; tag<Byte.MAX_VALUE; tag++) {
        // We should initialize the SerDe with the TypeInfo when available.
        tableDesc valueTableDesc = PlanUtils.getReduceValueDesc(gWork, tag);
        inputValueDeserializer[tag] = (SerDe)ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(), null);
        inputValueDeserializer[tag].initialize(null, valueTableDesc.getProperties());
      }
    } catch (SerDeException e) {
      throw new RuntimeException(e);
    }    
  }

  private Object keyObject;
  private ObjectInspector keyObjectInspector;
  private Object[] valueObject = new Object[Byte.MAX_VALUE];
  private ObjectInspector[] valueObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
  private ObjectInspector[] rowObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
  
  private BytesWritable groupKey;
  
  ArrayList<Object> row = new ArrayList<Object>(3);
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
      BytesWritable keyWritable = (BytesWritable)key;
      byte tag = 0;
      if (isTagged) {
        // remove the tag
        int size = keyWritable.getSize() - 1;
        tag = keyWritable.get()[size]; 
        keyWritable.setSize(size);
      }
      
      if (!keyWritable.equals(groupKey)) {
        // If a operator wants to do some work at the beginning of a group
        if (groupKey == null) {
          groupKey = new BytesWritable();
        } else {
          // If a operator wants to do some work at the end of a group
          l4j.trace("End Group");
          reducer.endGroup();
        }
        groupKey.set(keyWritable.get(), 0, keyWritable.getSize());
        l4j.trace("Start Group");
        reducer.startGroup();
      }
      try {
        keyObject = inputKeyDeserializer.deserialize(keyWritable);
      } catch (SerDeException e) {
        throw new HiveException(e);
      }
      // This is a hack for generating the correct ObjectInspector.
      // In the future, we should use DynamicSerde and initialize it using the type info. 
      if (keyObjectInspector == null) {
        // Directly create ObjectInspector here because we didn't know the number of cols till now.
        keyObjectInspector = MetadataListStructObjectInspector.getInstance(((ColumnSet)keyObject).col.size()); 
      }
      // System.err.print(keyObject.toString());
      while (values.hasNext()) {
        Text valueText = (Text)values.next();
        //System.err.print(who.getHo().toString());
        try {
          valueObject[tag] = inputValueDeserializer[tag].deserialize(valueText);
        } catch (SerDeException e) {
          throw new HiveException(e);
        }
        row.clear();
        row.add(keyObject);
        row.add(valueObject[tag]);
        row.add(tag);
        if (valueObjectInspector[tag] == null) {
          // Directly create ObjectInspector here because we didn't know the number of cols till now.
          valueObjectInspector[tag] = MetadataListStructObjectInspector.getInstance(((ColumnSet)valueObject[tag]).col.size());
          ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
          ois.add(keyObjectInspector);
          ois.add(valueObjectInspector[tag]);
          ois.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Byte.class));
          rowObjectInspector[tag] = ObjectInspectorFactory.getStandardStructObjectInspector(
              Arrays.asList(fieldNames), ois);
        }
        reducer.process(row, rowObjectInspector[tag]);
      }


    } catch (HiveException e) {
      abort = true;
      throw new IOException (e.getMessage());
    }
  }

  public void close() {

    // No row was processed
    if(oc == null) {
      try {
        l4j.trace("Close called no row");
        reducer.initialize(jc);
        rp = null;
      } catch (HiveException e) {
        abort = true;
        e.printStackTrace();
        throw new RuntimeException ("Reduce operator close failed during initialize", e);
      }
    }

    try {
      if (groupKey != null) {
        // If a operator wants to do some work at the end of a group
        l4j.trace("End Group");
        reducer.endGroup();
      }
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
