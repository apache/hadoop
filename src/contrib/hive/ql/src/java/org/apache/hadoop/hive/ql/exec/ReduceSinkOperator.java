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
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.Text;

/**
 * Reduce Sink Operator sends output to the reduce stage
 **/
public class ReduceSinkOperator extends TerminalOperator <reduceSinkDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  transient protected ExprNodeEvaluator[] keyEval;
  transient protected ExprNodeEvaluator[] valueEval;
  
  // TODO: we use MetadataTypedColumnsetSerDe for now, till DynamicSerDe is ready
  transient Serializer keySerializer;
  transient Serializer valueSerializer;
  transient int tag;
  transient byte[] tagByte = new byte[1];
  transient int numPartitionFields; 
  
  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    try {
      keyEval = new ExprNodeEvaluator[conf.getKeyCols().size()];
      int i=0;
      for(exprNodeDesc e: conf.getKeyCols()) {
        keyEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      valueEval = new ExprNodeEvaluator[conf.getValueCols().size()];
      i=0;
      for(exprNodeDesc e: conf.getValueCols()) {
        valueEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      tag = conf.getTag();
      tagByte[0] = (byte)tag;
      LOG.info("Using tag = " + tag);

      tableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer)keyTableDesc.getDeserializerClass().newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      
      tableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer)valueTableDesc.getDeserializerClass().newInstance();
      valueSerializer.initialize(null, valueTableDesc.getProperties());
      
      // Set the number of key fields to be used in the partitioner.
      numPartitionFields = conf.getNumPartitionFields();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  transient InspectableObject tempInspectableObject = new InspectableObject();
  transient HiveKey keyWritable = new HiveKey();
  transient Text valueText;
  
  transient ObjectInspector keyObjectInspector;
  transient ObjectInspector valueObjectInspector;
  transient ArrayList<ObjectInspector> keyFieldsObjectInspectors = new ArrayList<ObjectInspector>();
  transient ArrayList<ObjectInspector> valueFieldsObjectInspectors = new ArrayList<ObjectInspector>();
  
  public void process(Object row, ObjectInspector rowInspector) throws HiveException {
    // TODO: use DynamicSerDe when that is ready
    try {
      // Generate hashCode for the tuple
      int keyHashCode = 0;
      if (numPartitionFields == -1) {
        keyHashCode = (int)(Math.random() * Integer.MAX_VALUE);
      }
      ArrayList<Object> keys = new ArrayList<Object>(keyEval.length);
      for(ExprNodeEvaluator e: keyEval) {
        e.evaluate(row, rowInspector, tempInspectableObject);
        keys.add(tempInspectableObject.o);
        if (numPartitionFields == keys.size()) {
          keyHashCode = keys.hashCode();
        }
        if (keyObjectInspector == null) {
          keyFieldsObjectInspectors.add(tempInspectableObject.oi);
        }
      }
      if (numPartitionFields > keys.size()) {
        keyHashCode = keys.hashCode();
      }
      if (keyObjectInspector == null) {
        keyObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
            ObjectInspectorUtils.getIntegerArray(keyFieldsObjectInspectors.size()),
            keyFieldsObjectInspectors);
      }
      Text key = (Text)keySerializer.serialize(keys, keyObjectInspector);
      if (tag == -1) {
        keyWritable.set(key.getBytes(), 0, key.getLength());
      } else {
        int keyLength = key.getLength();
        keyWritable.setSize(keyLength+1);
        System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
        keyWritable.get()[keyLength] = tagByte[0];
      }
      keyWritable.setHashCode(keyHashCode);
      
      ArrayList<Object> values = new ArrayList<Object>(valueEval.length);
      for(ExprNodeEvaluator e: valueEval) {
        e.evaluate(row, rowInspector, tempInspectableObject);
        values.add(tempInspectableObject.o);
        if (valueObjectInspector == null) {
          valueFieldsObjectInspectors.add(tempInspectableObject.oi);
        }
      }
      if (valueObjectInspector == null) {
        valueObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
            ObjectInspectorUtils.getIntegerArray(valueFieldsObjectInspectors.size()),
            valueFieldsObjectInspectors);
      }
      valueText = (Text)valueSerializer.serialize(values, valueObjectInspector);
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
    try {
      out.collect(keyWritable, valueText);
    } catch (IOException e) {
      throw new HiveException (e);
    }
  }
}
