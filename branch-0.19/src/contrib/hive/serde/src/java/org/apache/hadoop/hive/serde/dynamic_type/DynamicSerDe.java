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

package org.apache.hadoop.hive.serde.dynamic_type;

import org.apache.hadoop.hive.serde.*;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;

import com.facebook.thrift.*;
import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

public class DynamicSerDe implements SerDe, Serializable {

  private String type_name;
  private DynamicSerDeStructBase bt;

  transient private thrift_grammar parse_tree;
  transient private boolean inStreaming;
  transient protected ByteStream.Input bis_;
  transient protected ByteStream.Output bos_;
  transient private TProtocol oprot_;
  transient private TProtocol iprot_;

  public static final String META_TABLE_NAME = "name";

    static public void main(String args[]) {
        try {


          {
            String schema_file = args[0];
            String kv_file = args[1];

            Properties schema = new Properties();
            schema.load(new
                        FileInputStream(schema_file));
            System.out.println(schema.toString());

            DynamicSerDe serde = new DynamicSerDe();
            serde.initialize(new Configuration(), schema);

            BufferedReader r = new BufferedReader(new FileReader(kv_file));
            String row;
            SerDeField keyF = serde.getFieldFromExpression(null, "key");
            while((row = r.readLine()) != null) {
              Text t = new Text(row);
              System.out.println("row = " + row);
              System.out.flush();
              Object o = serde.deserialize(t);
              System.out.println(o.toString() + " of type " +
                                 o.getClass().getName());
              Object fo = keyF.get(o);
            }
            if(true) return;

          }



            Properties schema = new Properties();
            String ddl = "struct test { i32 hello, list<string> bye, set<i32> more, map<string,bool> another}" ;

            schema.setProperty(Constants.SERIALIZATION_DDL,ddl);
            schema.setProperty(Constants.SERIALIZATION_LIB, new DynamicSerDe().getClass().toString());
            schema.setProperty(Constants.SERIALIZATION_FORMAT, "com.facebook.thrift.protocol.TJSONProtocol");
            schema.setProperty(META_TABLE_NAME,"test");

            DynamicSerDe serde = new DynamicSerDe();
            serde.initialize(new Configuration(), schema);

            DynamicSerDeTypeContainer data = new  DynamicSerDeTypeContainer();
            ArrayList<String> hellos = new ArrayList<String>();
            hellos.add("goodbye and this is more stuff - what is going oin here this is really really weird");

            Set<Integer> set = new HashSet<Integer>();
            set.add(22);

            Map<String,Boolean> map = new HashMap<String,Boolean>();
            map.put("me",true);

            data.fields.put("bye",hellos);
            data.fields.put("hello",Integer.valueOf(10032));
            data.fields.put("more",set);
            data.fields.put("another",map);

            BytesWritable foo = (BytesWritable)serde.serialize(data);
            System.err.println(new String(foo.get()));

            Object obj = serde.deserialize(foo);
            System.err.println("obj=" + obj);

        } catch(Exception e) {
            System.err.println("got exception: " + e.getMessage());
            e.printStackTrace();
        }
   }
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

            // in theory the include path should come from the configuration
            List<String> include_path = new ArrayList<String>();
            include_path.add(".");
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

            this.inStreaming = job.get("hive.streaming.select") != null;
        } catch (Exception e) {
            System.out.println(StringUtils.stringifyException(e));
            throw new SerDeException(e);
        }
    }

    public DynamicSerDeTypeContainer deserialize(Writable field) throws SerDeException {
        try {
            Text b = (Text)field;
            bis_.reset(b.getBytes(), b.getLength());
            return this.bt.deserialize(iprot_);
        } catch(Exception e) {
            e.printStackTrace();
            throw new SerDeException(e);
        }

    }

    public Writable serialize(Object o) throws SerDeException {
        DynamicSerDeTypeContainer obj = (DynamicSerDeTypeContainer)o;

        try {
            this.bt.serialize(obj, oprot_);
            oprot_.getTransport().flush();
        } catch(Exception e) {
            e.printStackTrace();
            throw new SerDeException(e);
        }
        return new BytesWritable(bos_.getData());
    }


    public SerDeField getFieldFromExpression(SerDeField parentField, String fieldExpression)
        throws SerDeException {

        DynamicSerDeStructBase type = bt;
        DynamicSerDeHiveField parentFieldCast = (DynamicSerDeHiveField)parentField;

        if(parentFieldCast != null) {
            if(parentFieldCast.isList()) {
                DynamicSerDeTypeBase elemType = parentFieldCast.getListElementMetaType();
                if(elemType instanceof DynamicSerDeStructBase) {
                    type = (DynamicSerDeStructBase)elemType;
                } else {
                    throw new SerDeException("Trying to get fields from a non struct/table type: " + elemType);
                }
            } else {
                DynamicSerDeTypeBase metaType = parentFieldCast.getMetaType();
                if(metaType instanceof DynamicSerDeStructBase) {
                    type = (DynamicSerDeStructBase)metaType;
                } else {
                    throw new SerDeException("Trying to get fields from a non struct/table type: " + metaType);
                }
            }
        }
        SerDeField field = new DynamicSerDeHiveField(type, fieldExpression);
        return field;
    }

    public List<SerDeField> getFields(SerDeField parentField)  throws SerDeException {
        DynamicSerDeStructBase type = bt;
        DynamicSerDeHiveField parentFieldCast = (DynamicSerDeHiveField)parentField;

        if(parentFieldCast != null) {
            DynamicSerDeTypeBase t = parentFieldCast.getMetaType();
            if(t instanceof DynamicSerDeStructBase) {
                type = (DynamicSerDeStructBase)t;
            } else {
                throw new SerDeException("trying to getFields on a non struct type: " + t);
            }
        }

        List<SerDeField> fields = new ArrayList<SerDeField>();

        for(DynamicSerDeField elem: type.getFieldList().getChildren()) {
            fields.add(this.getFieldFromExpression(parentField, elem.name));
        }
        return fields;
    }

    public String toJSONString(Object obj, SerDeField hf) throws SerDeException {
        //        return(tsd.toJSONString(obj, hf));
        return null;
    }
}
