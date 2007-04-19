/**
 * Copyright 2006 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HRegionInfo implements Writable {
  public long regionId;
  public HTableDescriptor tableDesc;
  public Text startKey;
  public Text endKey;
  public Text regionName;
  
  public HRegionInfo() {
    this.regionId = 0;
    this.tableDesc = new HTableDescriptor();
    this.startKey = new Text();
    this.endKey = new Text();
    this.regionName = new Text();
  }

  public HRegionInfo(long regionId, HTableDescriptor tableDesc, Text startKey, 
                     Text endKey) throws IllegalArgumentException {
    
    this.regionId = regionId;
    
    if (tableDesc == null) {
      throw new IllegalArgumentException("tableDesc cannot be null");
    }
    
    this.tableDesc = tableDesc;
    
    this.startKey = new Text();
    if (startKey != null) {
      this.startKey.set(startKey);
    }
    
    this.endKey = new Text();
    if (endKey != null) {
      this.endKey.set(endKey);
    }
    
    this.regionName = new Text(tableDesc.getName() + "_"
                               + (startKey == null ? "" : startKey.toString()) + "_" + regionId);
  }
    
  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    out.writeLong(regionId);
    tableDesc.write(out);
    startKey.write(out);
    endKey.write(out);
    regionName.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.regionId = in.readLong();
    this.tableDesc.readFields(in);
    this.startKey.readFields(in);
    this.endKey.readFields(in);
    this.regionName.readFields(in);
  }
}
