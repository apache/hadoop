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
import java.util.List;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Reduce Sink Operator sends output to the reduce stage
 **/
public class ReduceSinkOperator extends TerminalOperator <reduceSinkDesc> implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * The evaluators for the key columns.
   * Key columns decide the sort order on the reducer side.
   * Key columns are passed to the reducer in the "key".
   */
  transient protected ExprNodeEvaluator[] keyEval;
  /**
   * The evaluators for the value columns.
   * Value columns are passed to reducer in the "value". 
   */
  transient protected ExprNodeEvaluator[] valueEval;
  /**
   * The evaluators for the partition columns (CLUSTER BY or DISTRIBUTE BY in Hive language).
   * Partition columns decide the reducer that the current row goes to.
   * Partition columns are not passed to reducer.
   */
  transient protected ExprNodeEvaluator[] partitionEval;
  
  // TODO: we use MetadataTypedColumnsetSerDe for now, till DynamicSerDe is ready
  transient Serializer keySerializer;
  transient boolean keyIsText;
  transient Serializer valueSerializer;
  transient int tag;
  transient byte[] tagByte = new byte[1];
  
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

      partitionEval = new ExprNodeEvaluator[conf.getPartitionCols().size()];
      i=0;
      for(exprNodeDesc e: conf.getPartitionCols()) {
        partitionEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      tag = conf.getTag();
      tagByte[0] = (byte)tag;
      LOG.info("Using tag = " + tag);

      tableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer)keyTableDesc.getDeserializerClass().newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);
      
      tableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer)valueTableDesc.getDeserializerClass().newInstance();
      valueSerializer.initialize(null, valueTableDesc.getProperties());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  transient InspectableObject tempInspectableObject = new InspectableObject();
  transient HiveKey keyWritable = new HiveKey();
  transient Writable value;
  
  transient ObjectInspector keyObjectInspector;
  transient ObjectInspector valueObjectInspector;
  transient ArrayList<ObjectInspector> keyFieldsObjectInspectors = new ArrayList<ObjectInspector>();
  transient ArrayList<ObjectInspector> valueFieldsObjectInspectors = new ArrayList<ObjectInspector>();
  
  public void process(Object row, ObjectInspector rowInspector) throws HiveException {
    try {
      // Evaluate the keys
      ArrayList<Object> keys = new ArrayList<Object>(keyEval.length);
      for(ExprNodeEvaluator e: keyEval) {
        e.evaluate(row, rowInspector, tempInspectableObject);
        keys.add(tempInspectableObject.o);
        // Construct the keyObjectInspector from the first row
        if (keyObjectInspector == null) {
          keyFieldsObjectInspectors.add(tempInspectableObject.oi);
        }
      }
      // Construct the keyObjectInspector from the first row
      if (keyObjectInspector == null) {
        keyObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
            ObjectInspectorUtils.getIntegerArray(keyFieldsObjectInspectors.size()),
            keyFieldsObjectInspectors);
      }
      // Serialize the keys and append the tag
      if (keyIsText) {
        Text key = (Text)keySerializer.serialize(keys, keyObjectInspector);
        if (tag == -1) {
          keyWritable.set(key.getBytes(), 0, key.getLength());
        } else {
          int keyLength = key.getLength();
          keyWritable.setSize(keyLength+1);
          System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
          keyWritable.get()[keyLength] = tagByte[0];
        }
      } else {
        // Must be BytesWritable
        BytesWritable key = (BytesWritable)keySerializer.serialize(keys, keyObjectInspector);
        if (tag == -1) {
          keyWritable.set(key.get(), 0, key.getSize());
        } else {
          int keyLength = key.getSize();
          keyWritable.setSize(keyLength+1);
          System.arraycopy(key.get(), 0, keyWritable.get(), 0, keyLength);
          keyWritable.get()[keyLength] = tagByte[0];
        }
      }
      // Set the HashCode
      int keyHashCode = 0;
      for(ExprNodeEvaluator e: partitionEval) {
        e.evaluate(row, rowInspector, tempInspectableObject);
        keyHashCode = keyHashCode * 31 
          + (tempInspectableObject.o == null ? 0 : tempInspectableObject.o.hashCode());
      }
      keyWritable.setHashCode(keyHashCode);
      
      // Evaluate the value
      ArrayList<Object> values = new ArrayList<Object>(valueEval.length);
      for(ExprNodeEvaluator e: valueEval) {
        e.evaluate(row, rowInspector, tempInspectableObject);
        values.add(tempInspectableObject.o);
        // Construct the valueObjectInspector from the first row
        if (valueObjectInspector == null) {
          valueFieldsObjectInspectors.add(tempInspectableObject.oi);
        }
      }
      // Construct the valueObjectInspector from the first row
      if (valueObjectInspector == null) {
        valueObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
            ObjectInspectorUtils.getIntegerArray(valueFieldsObjectInspectors.size()),
            valueFieldsObjectInspectors);
      }
      // Serialize the value
      value = valueSerializer.serialize(values, valueObjectInspector);
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
    
    try {
      out.collect(keyWritable, value);
    } catch (IOException e) {
      throw new HiveException (e);
    }
  }
  
  public List<String> genColLists(HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) 
    throws SemanticException {
    RowResolver redSinkRR = opParseCtx.get(this).getRR();
    List<String> childColLists = new ArrayList<String>();

    for(Operator<? extends Serializable> o: childOperators)
      childColLists = Utilities.mergeUniqElems(childColLists, o.genColLists(opParseCtx));

    List<String> colLists = new ArrayList<String>();
    ArrayList<exprNodeDesc> keys = conf.getKeyCols();
    for (exprNodeDesc key : keys)
      colLists = Utilities.mergeUniqElems(colLists, key.getCols());

    // In case of extract child, see the columns used and propagate them
    if ((childOperators.size() == 1) && (childOperators.get(0) instanceof ExtractOperator)) {
      assert parentOperators.size() == 1;
      Operator<? extends Serializable> par = parentOperators.get(0);
      RowResolver parRR = opParseCtx.get(par).getRR();

      for (String childCol : childColLists) {
        String [] nm = redSinkRR.reverseLookup(childCol);
        ColumnInfo cInfo = parRR.get(nm[0],nm[1]);
        if (!colLists.contains(cInfo.getInternalName()))
          colLists.add(cInfo.getInternalName());
      }
    }
    else if ((childOperators.size() == 1) && (childOperators.get(0) instanceof JoinOperator)) {
      assert parentOperators.size() == 1;
      Operator<? extends Serializable> par = parentOperators.get(0);
      RowResolver parRR = opParseCtx.get(par).getRR();
      RowResolver childRR = opParseCtx.get(childOperators.get(0)).getRR();

      for (String childCol : childColLists) {
        String [] nm = childRR.reverseLookup(childCol);
        ColumnInfo cInfo = redSinkRR.get(nm[0],nm[1]);
        if (cInfo != null) {
          cInfo = parRR.get(nm[0], nm[1]);
          if (!colLists.contains(cInfo.getInternalName()))
            colLists.add(cInfo.getInternalName());
        }
      }
    }
    else {
      
      // Reduce Sink contains the columns needed - no need to aggregate from children
      ArrayList<exprNodeDesc> vals = conf.getValueCols();
      for (exprNodeDesc val : vals)
        colLists = Utilities.mergeUniqElems(colLists, val.getCols());
    }

    OpParseContext ctx = opParseCtx.get(this);
    ctx.setColNames(colLists);
    opParseCtx.put(this, ctx);
    return colLists;
  }

}
