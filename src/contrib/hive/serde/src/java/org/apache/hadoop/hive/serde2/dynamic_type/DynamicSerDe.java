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

package org.apache.hadoop.hive.serde2.dynamic_type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import org.apache.hadoop.hive.serde2.thrift.ConfigurableTProtocol;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

public class DynamicSerDe implements SerDe, Serializable {

  public static final Log LOG = LogFactory.getLog(DynamicSerDe.class.getName());

  private String type_name;
  private DynamicSerDeStructBase bt;

  public static final String META_TABLE_NAME = "name";

  transient private thrift_grammar parse_tree;
  transient protected ByteStream.Input bis_;
  transient protected ByteStream.Output bos_;

  /**
   * protocols are protected in case any of their properties need to be queried from another
   * class in this package. For TCTLSeparatedProtocol for example, may want to query the separators.
   */
  transient protected TProtocol oprot_;
  transient protected TProtocol iprot_;

  TIOStreamTransport tios;

  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    try {

      String ddl = tbl.getProperty(Constants.SERIALIZATION_DDL);
      type_name = tbl.getProperty(META_TABLE_NAME);
      String protoName = tbl.getProperty(Constants.SERIALIZATION_FORMAT);

      if(protoName == null) {
        protoName = "com.facebook.thrift.protocol.TBinaryProtocol";
      }
      TProtocolFactory protFactory = TReflectionUtils.getProtocolFactoryByName(protoName);
      bos_ = new ByteStream.Output();
      bis_ = new ByteStream.Input();
      tios = new TIOStreamTransport(bis_,bos_);

      oprot_ = protFactory.getProtocol(tios);
      iprot_ = protFactory.getProtocol(tios);

      /**
       * initialize the protocols
       */

      if(oprot_ instanceof org.apache.hadoop.hive.serde2.thrift.ConfigurableTProtocol) {
        ((ConfigurableTProtocol)oprot_).initialize(job, tbl);
      }

      if(iprot_ instanceof org.apache.hadoop.hive.serde2.thrift.ConfigurableTProtocol) {
        ((ConfigurableTProtocol)iprot_).initialize(job, tbl);
      }

      // in theory the include path should come from the configuration
      List<String> include_path = new ArrayList<String>();
      include_path.add(".");
      LOG.debug("ddl=" + ddl);
      this.parse_tree = new thrift_grammar(new ByteArrayInputStream(ddl.getBytes()), include_path,false);
      this.parse_tree.Start();

      this.bt = (DynamicSerDeStructBase)this.parse_tree.types.get(type_name);

      if(this.bt == null) {
        this.bt = (DynamicSerDeStructBase)this.parse_tree.tables.get(type_name);
      }

      if(this.bt == null) {
        throw new SerDeException("Could not lookup table type " + type_name + " in this ddl: " + ddl);
      }

      this.bt.initialize();
    } catch (Exception e) {
      System.out.println(StringUtils.stringifyException(e));
      throw new SerDeException(e);
    }
  }

  Object deserializeReuse = null;
  public Object deserialize(Writable field) throws SerDeException {
    try {
      if (field instanceof Text) {
        Text b = (Text)field;
        bis_.reset(b.getBytes(), b.getLength());
      } else {
        BytesWritable b = (BytesWritable)field;
        bis_.reset(b.get(), b.getSize());
      }
      deserializeReuse = this.bt.deserialize(deserializeReuse, iprot_);
      return deserializeReuse;
    } catch(Exception e) {
      e.printStackTrace();
      throw new SerDeException(e);
    }
  }

  public static ObjectInspector dynamicSerDeStructBaseToObjectInspector(DynamicSerDeTypeBase bt) throws SerDeException {
    if (bt.isList()) {
      return ObjectInspectorFactory.getStandardListObjectInspector(
          dynamicSerDeStructBaseToObjectInspector(((DynamicSerDeTypeList)bt).getElementType()));
    } else if (bt.isMap()) {
      DynamicSerDeTypeMap btMap = (DynamicSerDeTypeMap)bt; 
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          dynamicSerDeStructBaseToObjectInspector(btMap.getKeyType()), 
          dynamicSerDeStructBaseToObjectInspector(btMap.getValueType()));
    } else if (bt.isPrimitive()) {
      return ObjectInspectorFactory.getStandardPrimitiveObjectInspector(bt.getRealType());
    } else {
      // Must be a struct
      DynamicSerDeStructBase btStruct = (DynamicSerDeStructBase)bt;
      DynamicSerDeFieldList fieldList = btStruct.getFieldList();
      DynamicSerDeField[] fields = fieldList.getChildren();
      ArrayList<String> fieldNames = new ArrayList<String>(fields.length); 
      ArrayList<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(fields.length); 
      for(int i=0; i<fields.length; i++) {
        fieldNames.add(fields[i].name);
        fieldObjectInspectors.add(
            dynamicSerDeStructBaseToObjectInspector(fields[i].getFieldType().getMyType()));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldObjectInspectors);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return dynamicSerDeStructBaseToObjectInspector(this.bt);
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  BytesWritable ret = new BytesWritable();
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
  throws SerDeException {
    try {
      bos_.reset();
      this.bt.serialize(obj, objInspector, oprot_);
      oprot_.getTransport().flush();
    } catch(Exception e) {
      e.printStackTrace();
      throw new SerDeException(e);
    }
    ret.set(bos_.getData(),0,bos_.getCount());
    return ret;
  }
}
