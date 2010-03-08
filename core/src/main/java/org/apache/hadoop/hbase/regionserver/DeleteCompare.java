/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Class that provides static method needed when putting deletes into memstore 
 */
public class DeleteCompare {
  
  /**
   * Return codes from deleteCompare.
   */
  enum DeleteCode {
    /**
     * Do nothing.  Move to next KV in memstore
     */
    SKIP,
    
    /**
     * Add to the list of deletes.
     */
    DELETE,
    
    /**
     * Stop looking at KVs in memstore.  Finalize.
     */
    DONE
  }

  /**
   * Method used when putting deletes into memstore to remove all the previous
   * entries that are affected by this Delete
   * @param mem
   * @param deleteBuffer
   * @param deleteRowOffset
   * @param deleteRowLength
   * @param deleteQualifierOffset
   * @param deleteQualifierLength
   * @param deleteTimeOffset
   * @param deleteType
   * @param comparator
   * @return SKIP if current KeyValue should not be deleted, DELETE if
   * current KeyValue should be deleted and DONE when the current KeyValue is
   * out of the Deletes range
   */
  public static DeleteCode deleteCompare(KeyValue mem, byte [] deleteBuffer,
      int deleteRowOffset, short deleteRowLength, int deleteQualifierOffset,
      int deleteQualifierLength, int deleteTimeOffset, byte deleteType,
      KeyValue.KeyComparator comparator) {

    //Parsing new KeyValue
    byte [] memBuffer = mem.getBuffer();
    int memOffset = mem.getOffset();

    //Getting key lengths
    int memKeyLen = Bytes.toInt(memBuffer, memOffset);
    memOffset += Bytes.SIZEOF_INT;

    //Skipping value lengths
    memOffset += Bytes.SIZEOF_INT;

    //Getting row lengths
    short memRowLen = Bytes.toShort(memBuffer, memOffset);
    memOffset += Bytes.SIZEOF_SHORT;
    int res = comparator.compareRows(memBuffer, memOffset, memRowLen,
        deleteBuffer, deleteRowOffset, deleteRowLength);
    if(res > 0) {
      return DeleteCode.DONE;
    } else if(res < 0){
      return DeleteCode.SKIP;
    }

    memOffset += memRowLen;

    //Getting family lengths
    byte memFamLen = memBuffer[memOffset];
    memOffset += Bytes.SIZEOF_BYTE + memFamLen;

    //Get column lengths
    int memQualifierLen = memKeyLen - memRowLen - memFamLen -
      Bytes.SIZEOF_SHORT - Bytes.SIZEOF_BYTE - Bytes.SIZEOF_LONG -
      Bytes.SIZEOF_BYTE;

    //Compare timestamp
    int tsOffset = memOffset + memQualifierLen;
    int timeRes = Bytes.compareTo(memBuffer, tsOffset, Bytes.SIZEOF_LONG,
        deleteBuffer, deleteTimeOffset, Bytes.SIZEOF_LONG);

    if (deleteType == KeyValue.Type.DeleteFamily.getCode()) {
      if (timeRes <= 0) {
        return DeleteCode.DELETE;
      }
      return DeleteCode.SKIP;
    }

    //Compare columns
    res = Bytes.compareTo(memBuffer, memOffset, memQualifierLen,
        deleteBuffer, deleteQualifierOffset, deleteQualifierLength);
    if (res < 0) {
      return DeleteCode.SKIP;
    } else if(res > 0) {
      return DeleteCode.DONE;
    }
    // same column, compare the time.
    if (timeRes == 0) {
      return DeleteCode.DELETE;
    } else if (timeRes < 0) {
      if (deleteType == KeyValue.Type.DeleteColumn.getCode()) {
        return DeleteCode.DELETE;
      }
      return DeleteCode.DONE;
    } else {
      return DeleteCode.SKIP;
    }
  } 
}
