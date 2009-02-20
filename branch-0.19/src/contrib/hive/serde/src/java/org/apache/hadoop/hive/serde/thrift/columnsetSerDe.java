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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.ColumnSet;
import org.apache.hadoop.hive.serde.ComplexSerDeField;
import org.apache.hadoop.hive.serde.ExpressionUtils;
import org.apache.hadoop.hive.serde.ReflectionSerDeField;
import org.apache.hadoop.hive.serde.SerDe;
import org.apache.hadoop.hive.serde.SerDeException;
import org.apache.hadoop.hive.serde.SerDeField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.facebook.thrift.protocol.TBinaryProtocol;

public class columnsetSerDe  extends ThriftByteStreamTypedSerDe implements SerDe {

  protected boolean inStreaming;
  private String separator;

  public columnsetSerDe () throws SerDeException {
    this(org.apache.hadoop.hive.serde.ColumnSet.class);
  }

  public columnsetSerDe (Class<?> argType) throws SerDeException {
    // fill super with dummies
    super(argType, new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory());
    separator = "\001";
  }

  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    inStreaming = job.get("hive.streaming.select") != null;
    String alt_sep = tbl.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT);
    if(alt_sep != null && alt_sep.length() > 0) {
      try {
        byte b [] = new byte[1];
        b[0] = Byte.valueOf(alt_sep).byteValue();
        separator = new String(b);
      } catch(NumberFormatException e) {
        separator = alt_sep;
      }
    }
  }

  public static Object deserialize(ColumnSet c, String row, String sep) throws Exception {
    c.col.clear();
    String [] l1 = row.split(sep, -1);

    for(String s: l1) {
      c.col.add(s);
    }
    return (c);
  }

  public Object deserialize(Writable field) throws SerDeException {
    ColumnSet c = new ColumnSet(new ArrayList<String>());
    try {
      try {
        Text tw = (Text)field;
        String row = tw.toString();
        return(deserialize(c, row, separator));
      } catch (ClassCastException e) {
        throw new SerDeException("columnsetSerDe  expects Text", e);
      } catch (Exception e) {
        throw new SerDeException(e);
      }
    } catch (SerDeException e) {

      // don't want to crap out streaming jobs because of one error.
      if(inStreaming) {
        return (c);
      } else {
        throw (e);
      }
    }
  }

  public Writable serialize(Object obj) throws SerDeException {
    throw new SerDeException("Not implemented yet");
  }

  public SerDeField getFieldFromExpression(SerDeField parentField, String fieldExpression) throws SerDeException {
    if(ExpressionUtils.isComplexExpression(fieldExpression)) {
      return  (new ComplexSerDeField(parentField, fieldExpression, this));
    } else {
      // ok - we know there's no nesting possible. this is columnset after all :-)
      // also we don't want to check for __isset crap
      String className = type.getName();
      return (new ReflectionSerDeField(className, fieldExpression));
    }
  }

  public static void main(String[] args) throws IOException, SerDeException {
    Text tw = new Text ();
    columnsetSerDe  csd = new columnsetSerDe();
    BufferedReader in = new BufferedReader (new InputStreamReader (System.in));

    String str = in.readLine();
    while(str != null) {
      //System.out.println(str);
      tw.set(str);
      System.out.println(csd.deserialize(tw).toString());
      str = in.readLine();
    }
  }
}
