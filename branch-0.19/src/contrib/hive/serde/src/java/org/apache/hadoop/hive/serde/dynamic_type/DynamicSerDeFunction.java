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
import com.facebook.thrift.protocol.TType.*;

public class DynamicSerDeFunction extends DynamicSerDeStructBase {

    // production is: Async() FunctionType() NAME FieldList() Throws() [CommaOrSemicolon]

    private final int FD_ASYNC = 0;
    private final int FD_FUNCTION_TYPE = 1;
    private final int FD_FIELD_LIST = 2;
    private final int FD_THROWS = 3;

    public DynamicSerDeFunction(int i) {
        super(i);
    }
    public DynamicSerDeFunction(thrift_grammar p, int i) {
        super(p,i);
    }

    public DynamicSerDeFieldList getFieldList() {
        return (DynamicSerDeFieldList)this.jjtGetChild(FD_FIELD_LIST);
    }

    public String toString() {
        String result = "function " + this.name + " (";
        result += this.getFieldList().toString();
        result += ")";
        return result;
    }

    public byte getType() {
        return TMessageType.CALL;
    }

}
