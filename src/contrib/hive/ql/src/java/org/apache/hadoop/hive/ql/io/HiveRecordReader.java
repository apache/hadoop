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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.hive.ql.exec.ExecMapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;

public class HiveRecordReader<K extends WritableComparable, V extends Writable>  
  implements RecordReader<K, V> {

  private RecordReader recordReader;
  public HiveRecordReader(RecordReader recordReader){ 
    this.recordReader = recordReader;
  }

  public void close() throws IOException { 
    recordReader.close(); 
  }

  public K createKey() { 
    return (K)recordReader.createKey();
  }

  public V createValue() { 
    return (V)recordReader.createValue();
  }

  public long getPos() throws IOException { 
    return recordReader.getPos();
  }

  public float getProgress() throws IOException { 
    return recordReader.getProgress();
  }
  
  public boolean  next(K key, V value) throws IOException { 
    if (ExecMapper.getDone())
      return false;
    return recordReader.next(key, value);
  }
}

