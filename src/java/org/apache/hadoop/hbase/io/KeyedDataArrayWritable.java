/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Wraps an array of KeyedData items as a Writable. The array elements
 * may be null.
 */
public class KeyedDataArrayWritable implements Writable {

  private final static KeyedData NULL_KEYEDDATA = new KeyedData();

  private KeyedData[] m_data;

  /**
   * Make a record of length 0
   */
  public KeyedDataArrayWritable() {
    m_data = new KeyedData[0];
  }

  /** @return the array of KeyedData */
  public KeyedData[] get() {
    return m_data; 
  }

  /**
   * Sets the KeyedData array
   * 
   * @param data array of KeyedData
   */
  public void set(KeyedData[] data) {
    if(data == null) {
      throw new NullPointerException("KeyedData[] cannot be null");
    }
    m_data = data;
  }

  // Writable
  
  public void readFields(DataInput in) throws IOException {
    int len = in.readInt();
    m_data = new KeyedData[len];
    for(int i = 0; i < len; i++) {
      m_data[i] = new KeyedData();
      m_data[i].readFields(in);
    }
  }

  public void write(DataOutput out) throws IOException {
    int len = m_data.length;
    out.writeInt(len);
    for(int i = 0; i < len; i++) {
      if(m_data[i] != null) {
        m_data[i].write(out);
      } else {
        NULL_KEYEDDATA.write(out);
      }
    }
  }
}
