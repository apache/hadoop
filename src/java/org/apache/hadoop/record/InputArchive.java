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
import java.io.ByteArrayOutputStream;
import org.apache.hadoop.io.Text;

/**
 * Interface that all the Deserializers have to implement.
 *
 * @author Milind Bhandarkar
 */
public interface InputArchive {
    public byte readByte(String tag) throws IOException;
    public boolean readBool(String tag) throws IOException;
    public int readInt(String tag) throws IOException;
    public long readLong(String tag) throws IOException;
    public float readFloat(String tag) throws IOException;
    public double readDouble(String tag) throws IOException;
    public Text readString(String tag) throws IOException;
    public ByteArrayOutputStream readBuffer(String tag) throws IOException;
    public void readRecord(Record r, String tag) throws IOException;
    public void startRecord(String tag) throws IOException;
    public void endRecord(String tag) throws IOException;
    public Index startVector(String tag) throws IOException;
    public void endVector(String tag) throws IOException;
    public Index startMap(String tag) throws IOException;
    public void endMap(String tag) throws IOException;
}
