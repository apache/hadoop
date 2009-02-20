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

package org.apache.hadoop.hive.serde.thrift;

import org.apache.hadoop.hive.serde.*;
import com.facebook.thrift.TException;
import com.facebook.thrift.TBase;
import com.facebook.thrift.TSerializer;
import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import java.lang.reflect.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;


public class ThriftByteStreamTypedSerDe extends ByteStreamTypedSerDe {

  protected TIOStreamTransport outTransport, inTransport;
  protected TProtocol outProtocol, inProtocol;

 private void init(TProtocolFactory inFactory, TProtocolFactory outFactory) throws Exception {
    outTransport = new TIOStreamTransport(bos);
    inTransport = new TIOStreamTransport(bis);
    outProtocol = outFactory.getProtocol(outTransport);
    inProtocol = inFactory.getProtocol(inTransport);
    json_serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
  }

  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    throw new SerDeException("ThriftByteStreamTypedSerDe is still semi-abstract");
  }

  public ThriftByteStreamTypedSerDe(Class<?> argType, TProtocolFactory inFactory,
                                    TProtocolFactory outFactory) throws SerDeException {
    super(argType);
    try {
      init(inFactory, outFactory);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  public ThriftByteStreamTypedSerDe(Class<?> argType) throws SerDeException {
    super(argType);
    throw new SerDeException("Constructor not supported");
  }


  public Object deserialize(Writable field) throws SerDeException {
    Object obj = super.deserialize(field);
    try {
      ((TBase)obj).read(inProtocol);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
    return (obj);
  }

  public Writable serialize(Object obj) throws SerDeException{
    super.serialize(obj);
    try {
      ((TBase)obj).write(outProtocol);
    } catch (Exception e) {
      throw new SerDeException(e);
    }

    // consider optimizing further - reuse a single BytesWritableobject pointing to
    // a different object everytime
    BytesWritable ret = new BytesWritable(bos.getData());
    ret.setSize(bos.size());
    return(ret);
  }

  public SerDeField getFieldFromExpression(SerDeField parentField, String fieldExpression) throws SerDeException {
    if(ExpressionUtils.isComplexExpression(fieldExpression)) {
      return  (new ComplexSerDeField(parentField, fieldExpression, this));
    } else {
      Class inClass = type;

      if(parentField != null) {
        // the parent field can be of list type. in which case, we want to evaluate
        // the fieldExpression relative to the contained class type.
        if(parentField.isList()) {
          inClass = parentField.getListElementType();
        } else {
          inClass = parentField.getType();
        }
      }
      String className = inClass.getName();

      if(com.facebook.thrift.TBase.class.isAssignableFrom(inClass)) {
        return (new ThriftSerDeField(className, fieldExpression));
      } else {
        System.err.println("Non TBase type "+inClass.getName()+" embedded inside TBase?");
        throw new SerDeException("Class "+className+"does not derive from TBase");
      }
    }
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
