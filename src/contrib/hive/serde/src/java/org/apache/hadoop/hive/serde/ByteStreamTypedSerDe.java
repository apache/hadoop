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

package org.apache.hadoop.hive.serde;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;

import com.facebook.thrift.TBase;
import com.facebook.thrift.TException;
import com.facebook.thrift.TSerializer;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Am abstract base class for serializing to/from byteswritable objects
 *
 */

public abstract class ByteStreamTypedSerDe extends TypedSerDe {

  protected ByteStream.Input bis;
  protected ByteStream.Output bos;

  protected BytesWritable cachedBw;
  protected TSerializer json_serializer;
    
  public ByteStreamTypedSerDe(Class<?> argType) throws SerDeException {
    super(argType);
    bos = new ByteStream.Output();
    bis = new ByteStream.Input();
    cachedBw = new BytesWritable();
    json_serializer = new TSerializer();
  }

  public Object deserialize(Writable field) throws SerDeException {
    Object retObj = super.deserialize(field);
    BytesWritable b = (BytesWritable)field;
    bis.reset(b.get(), b.getSize());
    return (retObj);
  }

  public Writable serialize(Object obj) throws SerDeException {
    // let the parent enforce any type constraints
    super.serialize(obj);
    bos.reset();
    return (cachedBw);
  }


  public List<SerDeField> getFields(SerDeField parentField) throws SerDeException {
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
      if(onef.getName().equals("__isset")) {
        continue;
      }
      ret.add(this.getFieldFromExpression(parentField, onef.getName()));
    }
    return(ret);
  }

  public String toJSONString(Object obj, SerDeField hf) throws SerDeException {
    try {
      if(obj == null) {
        return "";
      }
      if (hf == null) {
        // if this is a top level Thrift object
        return json_serializer.toString((TBase)obj);
      }
      if(hf.isList() || hf.isMap()) {
        // pretty print a list
        return SerDeUtils.toJSONString(obj, hf, this);
      }
      if(hf.isPrimitive()) {
        // escape string before printing
        return SerDeUtils.lightEscapeString(obj.toString());
        }

      // anything else must be a top level thrift object as well
      return json_serializer.toString((TBase)obj);
    } catch (TException e) {
      throw new SerDeException("toJSONString:TJSONProtocol error", e);
    }
  }
}
