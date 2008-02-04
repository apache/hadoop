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

package org.apache.hadoop.hbase.thrift;

import org.apache.hadoop.hbase.BloomFilterDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.io.Text;

public class ThriftUtilities {
  
  /**
   * This utility method creates a new Hbase HColumnDescriptor object based on a
   * Thrift ColumnDescriptor "struct".
   * 
   * @param in
   *          Thrift ColumnDescriptor object
   * @return HColumnDescriptor
   * @throws IllegalArgument
   */
  static public HColumnDescriptor colDescFromThrift(ColumnDescriptor in)
      throws IllegalArgument {
    CompressionType comp = CompressionType.valueOf(in.compression);
    BloomFilterDescriptor bloom = null;
    if (in.bloomFilterType.compareTo("NONE") != 0) {
      if (in.bloomFilterVectorSize > 0 && in.bloomFilterNbHashes > 0) {
        bloom = new BloomFilterDescriptor(BloomFilterDescriptor.BloomFilterType
            .valueOf(in.bloomFilterType), in.bloomFilterVectorSize,
            in.bloomFilterNbHashes);
      } else if (in.bloomFilterVectorSize > 0) {
        bloom = new BloomFilterDescriptor(BloomFilterDescriptor.BloomFilterType
            .valueOf(in.bloomFilterType), in.bloomFilterVectorSize);
      } else {
        throw new IllegalArgument(
            "must specify number of entries for bloom filter");
      }
    }
    
    if (in.name == null || in.name.length <= 0) {
      throw new IllegalArgument("column name is empty");
    }
    HColumnDescriptor col = new HColumnDescriptor(new Text(in.name),
        in.maxVersions, comp, in.inMemory, in.maxValueLength, bloom);
    return col;
  }
  
  /**
   * This utility method creates a new Thrift ColumnDescriptor "struct" based on
   * an Hbase HColumnDescriptor object.
   * 
   * @param in
   *          Hbase HColumnDescriptor object
   * @return Thrift ColumnDescriptor
   */
  static public ColumnDescriptor colDescFromHbase(HColumnDescriptor in) {
    ColumnDescriptor col = new ColumnDescriptor();
    col.name = in.getName().getBytes();
    col.maxVersions = in.getMaxVersions();
    col.compression = in.getCompression().toString();
    col.inMemory = in.isInMemory();
    col.maxValueLength = in.getMaxValueLength();
    BloomFilterDescriptor bloom = in.getBloomFilter();
    if (bloom != null) {
      col.bloomFilterType = bloom.getType().toString();
      col.bloomFilterVectorSize = bloom.getVectorSize();
      col.bloomFilterNbHashes = bloom.getNbHash();
    }
    return col;
  }
  
}
