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

package org.apache.hadoop.record;

import java.io.IOException;
import java.util.TreeMap;
import java.util.ArrayList;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CsvRecordOutput implements RecordOutput {

  private PrintStream stream;
  private boolean isFirst = true;
    
  private void throwExceptionOnError(String tag) throws IOException {
    if (stream.checkError()) {
      throw new IOException("Error serializing "+tag);
    }
  }
 
  private void printCommaUnlessFirst() {
    if (!isFirst) {
      stream.print(",");
    }
    isFirst = false;
  }
    
  /** Creates a new instance of CsvRecordOutput */
  public CsvRecordOutput(OutputStream out) {
    try {
      stream = new PrintStream(out, true, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }
    
  @Override
  public void writeByte(byte b, String tag) throws IOException {
    writeLong((long)b, tag);
  }
    
  @Override
  public void writeBool(boolean b, String tag) throws IOException {
    printCommaUnlessFirst();
    String val = b ? "T" : "F";
    stream.print(val);
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeInt(int i, String tag) throws IOException {
    writeLong((long)i, tag);
  }
    
  @Override
  public void writeLong(long l, String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print(l);
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeFloat(float f, String tag) throws IOException {
    writeDouble((double)f, tag);
  }
    
  @Override
  public void writeDouble(double d, String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print(d);
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeString(String s, String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print(Utils.toCSVString(s));
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeBuffer(Buffer buf, String tag)
    throws IOException {
    printCommaUnlessFirst();
    stream.print(Utils.toCSVBuffer(buf));
    throwExceptionOnError(tag);
  }
    
  @Override
  public void startRecord(Record r, String tag) throws IOException {
    if (tag != null && ! tag.isEmpty()) {
      printCommaUnlessFirst();
      stream.print("s{");
      isFirst = true;
    }
  }
    
  @Override
  public void endRecord(Record r, String tag) throws IOException {
    if (tag == null || tag.isEmpty()) {
      stream.print("\n");
      isFirst = true;
    } else {
      stream.print("}");
      isFirst = false;
    }
  }
    
  @Override
  public void startVector(ArrayList v, String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print("v{");
    isFirst = true;
  }
    
  @Override
  public void endVector(ArrayList v, String tag) throws IOException {
    stream.print("}");
    isFirst = false;
  }
    
  @Override
  public void startMap(TreeMap v, String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print("m{");
    isFirst = true;
  }
    
  @Override
  public void endMap(TreeMap v, String tag) throws IOException {
    stream.print("}");
    isFirst = false;
  }
}
