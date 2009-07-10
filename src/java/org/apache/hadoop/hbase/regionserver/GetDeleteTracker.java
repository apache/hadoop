/*
 * Copyright 2009 The Apache Software Foundation
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is responsible for the tracking and enforcement of Deletes
 * during the course of a Get operation.
 * <p>
 * This class is utilized through three methods:
 * <ul><li>{@link #add} when encountering a Delete
 * <li>{@link #isDeleted} when checking if a Put KeyValue has been deleted
 * <li>{@link #update} when reaching the end of a StoreFile
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded 
 */
public class GetDeleteTracker implements DeleteTracker {

  private long familyStamp = -1L;
  protected List<Delete> deletes = null;
  private List<Delete> newDeletes = new ArrayList<Delete>();
  private Iterator<Delete> iterator;
  private Delete delete = null;

  /**
   * Constructor
   */
  public GetDeleteTracker() {}

  /**
   * Add the specified KeyValue to the list of deletes to check against for
   * this row operation.
   * <p>
   * This is called when a Delete is encountered in a StoreFile.
   * @param buffer
   * @param qualifierOffset
   * @param qualifierLength
   * @param timestamp
   * @param type
   */
  @Override
  public void add(byte [] buffer, int qualifierOffset, int qualifierLength,
      long timestamp, byte type) {
    if(type == KeyValue.Type.DeleteFamily.getCode()) {
      if(timestamp > familyStamp) {
        familyStamp = timestamp;
      }
      return;
    }
    if(timestamp > familyStamp) {
      this.newDeletes.add(new Delete(buffer, qualifierOffset, qualifierLength,
          type, timestamp));
    }
  }

  /** 
   * Check if the specified KeyValue buffer has been deleted by a previously
   * seen delete.
   * @param buffer KeyValue buffer
   * @param qualifierOffset column qualifier offset
   * @param qualifierLength column qualifier length
   * @param timestamp timestamp
   * @return true is the specified KeyValue is deleted, false if not
   */
  @Override
  public boolean isDeleted(byte [] buffer, int qualifierOffset,
      int qualifierLength, long timestamp) {

    // Check against DeleteFamily
    if (timestamp <= familyStamp) {
      return true;
    }

    // Check if there are other deletes
    if(this.delete == null) {
      return false;
    }

    // Check column
    int ret = Bytes.compareTo(buffer, qualifierOffset, qualifierLength,
        this.delete.buffer, this.delete.qualifierOffset, 
        this.delete.qualifierLength);
    if(ret <= -1) {
      // Have not reached the next delete yet
      return false;
    } else if(ret >= 1) {
      // Deletes an earlier column, need to move down deletes
      if(this.iterator.hasNext()) {
        this.delete = this.iterator.next();
      } else {
        this.delete = null;
        return false;
      }
      return isDeleted(buffer, qualifierOffset, qualifierLength, timestamp);
    }

    // Check Timestamp
    if(timestamp > this.delete.timestamp) {
      return false;
    }

    // Check Type
    switch(KeyValue.Type.codeToType(this.delete.type)) {
    case Delete:
      boolean equal = timestamp == this.delete.timestamp;

      if(this.iterator.hasNext()) {
        this.delete = this.iterator.next();
      } else {
        this.delete = null;
      }

      if(equal){
        return true;
      }
      // timestamp < this.delete.timestamp
      // Delete of an explicit column newer than current
      return isDeleted(buffer, qualifierOffset, qualifierLength, timestamp);
    case DeleteColumn:
      return true;
    }

    // should never reach this
    return false;
  }

  @Override
  public boolean isEmpty() {
    if(this.familyStamp == 0L && this.delete == null) {
      return true;
    }
    return false;
  }

  @Override
  public void reset() {
    this.deletes = null;
    this.delete = null;
    this.newDeletes = new ArrayList<Delete>();
    this.familyStamp = 0L;
    this.iterator = null;
  }

  /**
   * Called at the end of every StoreFile.
   * <p>
   * Many optimized implementations of Trackers will require an update at
   * when the end of each StoreFile is reached.
   */
  @Override
  public void update() {
    // If no previous deletes, use new deletes and return
    if(this.deletes == null || this.deletes.size() == 0) {
      finalize(this.newDeletes);
      return;
    }

    // If no new delete, retain previous deletes and return
    if(this.newDeletes.size() == 0) {
      return;
    }

    // Merge previous deletes with new deletes
    List<Delete> mergeDeletes = 
      new ArrayList<Delete>(this.newDeletes.size());
    int oldIndex = 0;
    int newIndex = 0;

    Delete newDelete = newDeletes.get(oldIndex);
    Delete oldDelete = deletes.get(oldIndex);
    while(true) {
      switch(compareDeletes(oldDelete,newDelete)) {
      case NEXT_NEW: {
        if(++newIndex == newDeletes.size()) {
          // Done with new, add the rest of old to merged and return
          mergeDown(mergeDeletes, deletes, oldIndex);
          finalize(mergeDeletes);
          return;
        }
        newDelete = this.newDeletes.get(newIndex);
        break;
      }

      case INCLUDE_NEW_NEXT_NEW: {
        mergeDeletes.add(newDelete);
        if(++newIndex == newDeletes.size()) {
          // Done with new, add the rest of old to merged and return
          mergeDown(mergeDeletes, deletes, oldIndex);
          finalize(mergeDeletes);
          return;
        }
        newDelete = this.newDeletes.get(newIndex);
        break;
      }

      case INCLUDE_NEW_NEXT_BOTH: {
        mergeDeletes.add(newDelete);
        ++oldIndex;
        ++newIndex;
        if(oldIndex == deletes.size()) {
          if(newIndex == newDeletes.size()) {
            finalize(mergeDeletes);
            return;
          }
          mergeDown(mergeDeletes, newDeletes, newIndex);
          finalize(mergeDeletes);
          return;
        } else if(newIndex == newDeletes.size()) {
          mergeDown(mergeDeletes, deletes, oldIndex);
          finalize(mergeDeletes);
          return;
        }
        oldDelete = this.deletes.get(oldIndex);
        newDelete = this.newDeletes.get(newIndex);
        break;
      }

      case INCLUDE_OLD_NEXT_BOTH: {
        mergeDeletes.add(oldDelete);
        ++oldIndex;
        ++newIndex;
        if(oldIndex == deletes.size()) {
          if(newIndex == newDeletes.size()) {
            finalize(mergeDeletes);
            return;
          }
          mergeDown(mergeDeletes, newDeletes, newIndex);
          finalize(mergeDeletes);
          return;
        } else if(newIndex == newDeletes.size()) {
          mergeDown(mergeDeletes, deletes, oldIndex);
          finalize(mergeDeletes);
          return;
        }
        oldDelete = this.deletes.get(oldIndex);
        newDelete = this.newDeletes.get(newIndex);
        break;
      }

      case INCLUDE_OLD_NEXT_OLD: {
        mergeDeletes.add(oldDelete);
        if(++oldIndex == deletes.size()) {
          mergeDown(mergeDeletes, newDeletes, newIndex);
          finalize(mergeDeletes);
          return;
        }
        oldDelete = this.deletes.get(oldIndex);
        break;
      }

      case NEXT_OLD: {
        if(++oldIndex == deletes.size()) {
          // Done with old, add the rest of new to merged and return
          mergeDown(mergeDeletes, newDeletes, newIndex);
          finalize(mergeDeletes);
          return;
        }
        oldDelete = this.deletes.get(oldIndex);
      }
      }
    }
  }

  private void finalize(List<Delete> mergeDeletes) {
    this.deletes = mergeDeletes;
    this.newDeletes = new ArrayList<Delete>();
    if(this.deletes.size() > 0){
      this.iterator = deletes.iterator();
      this.delete = iterator.next();
    }
  }

  private void mergeDown(List<Delete> mergeDeletes, List<Delete> srcDeletes, 
      int srcIndex) {
    int index = srcIndex;
    while(index < srcDeletes.size()) {
      mergeDeletes.add(srcDeletes.get(index++));
    }
  }


  protected DeleteCompare compareDeletes(Delete oldDelete, Delete newDelete) {

    // Compare columns
    // Just compairing qualifier portion, can keep on using Bytes.compareTo().
    int ret = Bytes.compareTo(oldDelete.buffer, oldDelete.qualifierOffset,
        oldDelete.qualifierLength, newDelete.buffer, newDelete.qualifierOffset,
        newDelete.qualifierLength);

    if(ret <= -1) {
      return DeleteCompare.INCLUDE_OLD_NEXT_OLD;
    } else if(ret >= 1) {
      return DeleteCompare.INCLUDE_NEW_NEXT_NEW;
    }

    // Same column

    // Branches below can be optimized.  Keeping like this until testing
    // is complete.
    if(oldDelete.type == newDelete.type) {
      // the one case where we can merge 2 deletes -> 1 delete.
      if(oldDelete.type == KeyValue.Type.Delete.getCode()){
        if(oldDelete.timestamp > newDelete.timestamp) {
          return DeleteCompare.INCLUDE_OLD_NEXT_OLD;
        } else if(oldDelete.timestamp < newDelete.timestamp) {
          return DeleteCompare.INCLUDE_NEW_NEXT_NEW;
        } else {
          return DeleteCompare.INCLUDE_OLD_NEXT_BOTH;
        }
      }
      if(oldDelete.timestamp < newDelete.timestamp) {
        return DeleteCompare.INCLUDE_NEW_NEXT_BOTH;
      } 
      return DeleteCompare.INCLUDE_OLD_NEXT_BOTH;
    }

    // old delete is more specific than the new delete.
    // if the olddelete is newer then the newdelete, we have to
    //  keep it
    if(oldDelete.type < newDelete.type) {
      if(oldDelete.timestamp > newDelete.timestamp) {
        return DeleteCompare.INCLUDE_OLD_NEXT_OLD;
      } else if(oldDelete.timestamp < newDelete.timestamp) {
        return DeleteCompare.NEXT_OLD;
      } else {
        return DeleteCompare.NEXT_OLD;
      }
    }

    // new delete is more specific than the old delete.
    if(oldDelete.type > newDelete.type) {
      if(oldDelete.timestamp > newDelete.timestamp) {
        return DeleteCompare.NEXT_NEW;
      } else if(oldDelete.timestamp < newDelete.timestamp) {
        return DeleteCompare.INCLUDE_NEW_NEXT_NEW;
      } else {
        return DeleteCompare.NEXT_NEW;
      }
    }

    // Should never reach,
    // throw exception for assertion?
    throw new RuntimeException("GetDeleteTracker:compareDelete reached terminal state");
  }

  /**
   * Internal class used to store the necessary information for a Delete.
   * <p>
   * Rather than reparsing the KeyValue, or copying fields, this class points
   * to the underlying KeyValue buffer with pointers to the qualifier and fields
   * for type and timestamp.  No parsing work is done in DeleteTracker now.
   * <p>
   * Fields are public because they are accessed often, directly, and only
   * within this class.
   */
  protected class Delete {
    byte [] buffer;
    int qualifierOffset;
    int qualifierLength;
    byte type;
    long timestamp;
    /**
     * Constructor
     * @param buffer
     * @param qualifierOffset
     * @param qualifierLength
     * @param type
     * @param timestamp
     */
    public Delete(byte [] buffer, int qualifierOffset, int qualifierLength,
        byte type, long timestamp) {
      this.buffer = buffer;
      this.qualifierOffset = qualifierOffset;
      this.qualifierLength = qualifierLength;
      this.type = type;
      this.timestamp = timestamp;
    }
  }
}
