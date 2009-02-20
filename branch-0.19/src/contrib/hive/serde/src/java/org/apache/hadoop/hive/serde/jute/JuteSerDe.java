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

package org.apache.hadoop.hive.serde.jute;

import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.serde.thrift.*;
import java.util.*;
import java.lang.reflect.*;
import java.io.*;

// apache logging stuff
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.*;
import org.apache.hadoop.record.*;

public class JuteSerDe implements SerDe {

  protected Class<?> type;
  private static final Log LOG = LogFactory.getLog("hive.metastore");
  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    try {
      String className = tbl.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS);
      type = Class.forName(className);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }
  public Object deserialize(Writable field) throws SerDeException {
    try {
      Record r = (Record)(type.newInstance());
      DataInput in = new DataInputStream(new ByteArrayInputStream(new byte[100000]));
      field.readFields(in);
      BinaryRecordInput ri = new BinaryRecordInput(in);
      r.deserialize(ri,"");
      return r;
    } catch(Exception e) {
      LOG.error("jute serialize problem: " + e.getMessage());
      throw new SerDeException(e);
    }

  }
  public Writable serialize(Object obj) throws SerDeException {
    try {
      Record r = (Record)obj;
      DataOutput dos = new DataOutputStream(new ByteArrayOutputStream(10000));
      BinaryRecordOutput ro = new BinaryRecordOutput(dos);
      r.serialize(ro,"test");
      BytesWritable bw = new BytesWritable();
      bw.write(dos);
      return bw;
    } catch(IOException e) {
      LOG.error("jute serialize problem: " + e.getMessage());
      throw new SerDeException(e);
    }
  }
  public SerDeField getFieldFromExpression(SerDeField parentField, String fieldExpression)   throws SerDeException {
    if(ExpressionUtils.isComplexExpression(fieldExpression)) {
      return  (new ComplexSerDeField(parentField, fieldExpression, this));
    }
    Class inClass = type;
    LOG.error("getFieldFromExpression(" + parentField +","  + fieldExpression + ")");

    if(parentField != null) {
      // the parent field can be of list type. in which case, we want to evaluate
      // the fieldExpression relative to the contained class type.
      if(parentField.isList()) {
        inClass = parentField.getListElementType();
      } else  {
        inClass = parentField.getType();
      }
    }
    String className = inClass.getName();
    JuteSerDeField field = new JuteSerDeField(className, fieldExpression);
    return field;
  }
  public List<SerDeField> getFields(SerDeField parentField)  throws SerDeException {
    Class c = type;
    if(parentField != null) {
      if(parentField.isPrimitive() || parentField.isMap()) {
        throw new SerDeException("Trying to list fields of primitive or map");
      }
      if(parentField.isList()) {
        c = parentField.getListElementType();
      } else {
        c = parentField.getType();
      }
    }
    Field [] farr = c.getDeclaredFields();
    ArrayList<SerDeField> ret = new ArrayList<SerDeField> (farr.length-1);
    for(Field onef: farr) {
      ret.add(this.getFieldFromExpression(parentField, onef.getName()));
    }
    return ret;
  }
  public String toJSONString(Object obj, SerDeField hf) throws SerDeException {
    return null;
  }
}
