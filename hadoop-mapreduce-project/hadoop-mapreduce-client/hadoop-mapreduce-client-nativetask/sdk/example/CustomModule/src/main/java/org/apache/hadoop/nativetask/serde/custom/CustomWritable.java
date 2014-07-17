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
package org.apache.hadoop.nativetask.serde.custom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomWritable implements WritableComparable<CustomWritable>{
  
  private int Id_a;
  private long Id_b;

  public CustomWritable() {
    this.Id_a = 0;
    this.Id_b = 0;
  }
  
  public CustomWritable(int a, long b){
    this.Id_a = a;
    this.Id_b = b;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    Id_a = in.readInt();
    Id_b = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(Id_a);
    out.writeLong(Id_b);
  }

  @Override
  public int compareTo(CustomWritable that) {
    if(Id_a > that.Id_a){
      return 1;
    }
    if(Id_a < that.Id_a){
      return -1;
    }
    if(Id_b > that.Id_b){
      return 1;
    }
    if(Id_b < that.Id_b){
      return -1;
    }
    return 0;
  }
  
  @Override
  public String toString() {
    return Id_a + "\t" + Id_b;
  }

}
