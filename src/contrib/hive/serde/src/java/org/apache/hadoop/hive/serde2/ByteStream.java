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

package org.apache.hadoop.hive.serde2;

import java.io.*;

/**
 * Extensions to bytearrayinput/output streams
 *
 */
public class ByteStream {
  public static class Input extends ByteArrayInputStream {
    public byte[] getData() { return buf; }
    public int getCount() { return count;}
    public void reset(byte [] argBuf, int argCount) {
      buf = argBuf; mark = pos = 0; count = argCount;
    }
    public Input() {
      super(new byte [1]);
    }

    public Input(byte[] buf) {
      super(buf);
    }
    public Input(byte[] buf, int offset, int length) {
      super(buf, offset, length);
    }
  }
    
  public static class Output extends ByteArrayOutputStream {
    public byte[] getData() { return buf; }
    public int getCount() { return count;}

    public Output() { super(); }
    public Output(int size) { super(size); }
  }
}
