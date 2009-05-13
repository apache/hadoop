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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;

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
    Compression.Algorithm comp =
      Compression.getCompressionAlgorithmByName(in.compression.toLowerCase());
    boolean bloom = false;
    if (in.bloomFilterType.compareTo("NONE") != 0) {
      bloom = true;
    }
    
    if (in.name == null || in.name.length <= 0) {
      throw new IllegalArgument("column name is empty");
    }
    HColumnDescriptor col = new HColumnDescriptor(in.name,
        in.maxVersions, comp.getName(), in.inMemory, in.blockCacheEnabled,
        in.maxValueLength, in.timeToLive, bloom);
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
    col.name = in.getName();
    col.maxVersions = in.getMaxVersions();
    col.compression = in.getCompression().toString();
    col.inMemory = in.isInMemory();
    col.blockCacheEnabled = in.isBlockCacheEnabled();
    col.maxValueLength = in.getMaxValueLength();
    col.bloomFilterType = Boolean.toString(in.isBloomfilter());
    return col;
  }
  
  /**
   * This utility method creates a list of Thrift TCell "struct" based on
   * an Hbase Cell object. The empty list is returned if the input is null.
   * 
   * @param in
   *          Hbase Cell object
   * @return Thrift TCell array
   */
  static public List<TCell> cellFromHBase(Cell in) {
    List<TCell> list = new ArrayList<TCell>(1);
    if (in != null) {
      list.add(new TCell(in.getValue(), in.getTimestamp()));
    }
    return list;
  }

  /**
   * This utility method creates a list of Thrift TCell "struct" based on
   * an Hbase Cell array. The empty list is returned if the input is null.
   * @param in Hbase Cell array
   * @return Thrift TCell array
   */
  static public List<TCell> cellFromHBase(Cell[] in) {
    List<TCell> list = null;
    if (in != null) {
      list = new ArrayList<TCell>(in.length);
      for (int i = 0; i < in.length; i++) {
        list.add(new TCell(in[i].getValue(), in[i].getTimestamp()));
      }
    } else {
      list = new ArrayList<TCell>(0);
    }
    return list;
  }

  /**
   * This utility method creates a list of Thrift TRowResult "struct" based on
   * an Hbase RowResult object. The empty list is returned if the input is
   * null.
   * 
   * @param in
   *          Hbase RowResult object
   * @return Thrift TRowResult array
   */
  static public List<TRowResult> rowResultFromHBase(RowResult[] in) {
    List<TRowResult> results = new ArrayList<TRowResult>();
    for ( RowResult result_ : in) {
        if(null == result_) {
            continue;
        }
        TRowResult result = new TRowResult();
        result.row = result_.getRow();
        result.columns = new TreeMap<byte[], TCell>(Bytes.BYTES_COMPARATOR);
        for (Map.Entry<byte[], Cell> entry : result_.entrySet()){
            Cell cell = entry.getValue();
            result.columns.put(entry.getKey(),
                new TCell(cell.getValue(), cell.getTimestamp()));

        }
        results.add(result);
    }
    return results;
  }
  static public List<TRowResult> rowResultFromHBase(RowResult in) {
    RowResult [] result = { in };
    return rowResultFromHBase(result);
  }

}

