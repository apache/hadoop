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

import com.facebook.thrift.TException;
import com.facebook.thrift.TApplicationException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.*;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.server.TServer;
import com.facebook.thrift.server.*;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.transport.TServerTransport;
import java.util.*;
import java.io.*;
import org.apache.hadoop.hive.serde.*;
import java.lang.reflect.*;
import com.facebook.thrift.protocol.TType;

public class DynamicSerDeTypeMap extends DynamicSerDeTypeBase {

    public boolean isPrimitive() { return false; }
    public boolean isMap() { return true;}

    // production is: Map<FieldType(),FieldType()>

    private final byte FD_KEYTYPE = 0;
    private final byte FD_VALUETYPE = 1;

    // returns Map<?,?>
    public Class getRealType() {
        try {
            Class c = this.getKeyType().getRealType();
            Class c2 = this.getValueType().getRealType();
            Object o = c.newInstance();
            Object o2 = c2.newInstance();
            Map<?,?> l = Collections.singletonMap(o,o2);
            return l.getClass();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public DynamicSerDeTypeMap(int i) {
        super(i);
    }

    public DynamicSerDeTypeMap(thrift_grammar p, int i) {
        super(p,i);
    }

    public DynamicSerDeTypeBase getKeyType() {
        return (DynamicSerDeTypeBase)((DynamicSerDeFieldType)this.jjtGetChild(FD_KEYTYPE)).getMyType();
    }

    public DynamicSerDeTypeBase getValueType() {
        return (DynamicSerDeTypeBase)((DynamicSerDeFieldType)this.jjtGetChild(FD_VALUETYPE)).getMyType();
    }

    public String toString() {
        return "map<" + this.getKeyType().toString() + "," + this.getValueType().toString() + ">";
    }

    public Map<Object,Object> deserialize(TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {
            TMap themap = iprot.readMapBegin();
            HashMap<Object, Object> result = new HashMap<Object, Object> ();
            for(int i = 0; i < themap.size; i++) {
                Object key = this.getKeyType().deserialize(iprot);
                Object value = this.getValueType().deserialize(iprot);
                result.put(key,value);
            }

            // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
            iprot.readMapEnd();
            return result;
    }

    public void serialize(Object o, TProtocol oprot) throws TException, SerDeException, NoSuchFieldException,IllegalAccessException  {
        Map<Object,Object> map = (Map<Object,Object>)o;
        DynamicSerDeTypeBase keyType = this.getKeyType();
        DynamicSerDeTypeBase valueType = this.getValueType();
        oprot.writeMapBegin(new TMap(keyType.getType(),valueType.getType(),map.size()));
        for(Iterator i = map.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry it = (Map.Entry)i.next();
            Object key = it.getKey();
            Object value = it.getValue();
            keyType.serialize(key, oprot);
            valueType.serialize(value, oprot);
        }
        // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
        oprot.writeMapEnd();
    }

    public byte getType() {
        return TType.MAP;
    }
};

