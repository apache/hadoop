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

public class DynamicSerDeTypei16 extends DynamicSerDeTypeBase {

    public Class getRealType() { return Integer.valueOf(2).getClass(); }

    // production is: i16

    public DynamicSerDeTypei16(int i) {
        super(i);
    }
    public DynamicSerDeTypei16(thrift_grammar p, int i) {
        super(p,i);
    }

    public String toString() { return "i16"; }

    public Integer deserialize(TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {
        return Integer.valueOf(iprot.readI16());
    }

    public void serialize(final Object s, TProtocol oprot) throws TException, SerDeException, NoSuchFieldException,IllegalAccessException  {
        oprot.writeI16((Short)s);
    }
    public byte getType() {
        return TType.I16;
    }
}
