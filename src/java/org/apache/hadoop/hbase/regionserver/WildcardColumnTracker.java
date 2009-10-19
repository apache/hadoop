/**
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
import java.util.List;

import org.apache.hadoop.hbase.regionserver.QueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is used for the tracking and enforcement of columns and numbers 
 * of versions during the course of a Get or Scan operation, when all available
 * column qualifiers have been asked for in the query.
 * <p>
 * This class is utilized by {@link QueryMatcher} through two methods:
 * <ul><li>{@link #checkColumn} is called when a Put satisfies all other
 * conditions of the query.  This method returns a {@link MatchCode} to define
 * what action should be taken.
 * <li>{@link #update} is called at the end of every StoreFile or memstore.
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded 
 */
public class WildcardColumnTracker implements ColumnTracker {
  
  private int maxVersions;
  
  protected List<ColumnCount> columns;
  private int index;
  private ColumnCount column;
  
  private List<ColumnCount> newColumns; 
  private int newIndex;
  private ColumnCount newColumn;
  
  /**
   * Default constructor.
   * @param maxVersions maximum versions to return per columns
   */
  public WildcardColumnTracker(int maxVersions) {
    this.maxVersions = maxVersions;
    reset();
  }
  
  public void reset() {
    this.index = 0;
    this.column = null;
    this.columns = null;
    this.newColumns = new ArrayList<ColumnCount>();
    this.newIndex = 0;
    this.newColumn = null;
  }
  
  /**
   * Can never early-out from reading more storefiles in Wildcard case.
   */
  public boolean done() {
    return false;
  }

  // wildcard scanners never have column hints.
  public ColumnCount getColumnHint() {
    return null;
  }

  /**
   * Checks against the parameters of the query and the columns which have
   * already been processed by this query.
   * @param bytes KeyValue buffer
   * @param offset offset to the start of the qualifier
   * @param length length of the qualifier
   * @return MatchCode telling QueryMatcher what action to take
   */
  public MatchCode checkColumn(byte [] bytes, int offset, int length) {

    // Nothing to match against, add to new and include
    if(this.column == null && this.newColumn == null) {
      newColumns.add(new ColumnCount(bytes, offset, length, 1));
      this.newColumn = newColumns.get(newIndex);
      return MatchCode.INCLUDE;
    }
    
    // Nothing old, compare against new
    if(this.column == null && this.newColumn != null) {
      int ret = Bytes.compareTo(newColumn.getBuffer(), newColumn.getOffset(), 
          newColumn.getLength(), bytes, offset, length);
      
      // Same column
      if(ret == 0) {
        if(newColumn.increment() > this.maxVersions) {
          return MatchCode.SKIP;
        }
        return MatchCode.INCLUDE;
      }
      
      // Specified column is bigger than current column
      // Move down current column and check again
      if(ret <= -1) {
        if(++newIndex == newColumns.size()) {
          // No more, add to end and include
          newColumns.add(new ColumnCount(bytes, offset, length, 1));
          this.newColumn = newColumns.get(newIndex);
          return MatchCode.INCLUDE;
        }
        this.newColumn = newColumns.get(newIndex);
        return checkColumn(bytes, offset, length);
      }
      
      // ret >= 1
      // Specified column is smaller than current column
      // Nothing to match against, add to new and include
      newColumns.add(new ColumnCount(bytes, offset, length, 1));
      this.newColumn = newColumns.get(++newIndex);
      return MatchCode.INCLUDE;
    }
    
    // Nothing new, compare against old
    if(this.newColumn == null && this.column != null) {
      int ret = Bytes.compareTo(column.getBuffer(), column.getOffset(), 
          column.getLength(), bytes, offset, length);
      
      // Same column
      if(ret == 0) {
        if(column.increment() > this.maxVersions) {
          return MatchCode.SKIP;
        }
        return MatchCode.INCLUDE;
      }
      
      // Specified column is bigger than current column
      // Move down current column and check again
      if(ret <= -1) {
        if(++index == columns.size()) {
          // No more, add to new and include (new was empty prior to this)
          newColumns.add(new ColumnCount(bytes, offset, length, 1));
          this.newColumn = newColumns.get(newIndex);
          this.column = null;
          return MatchCode.INCLUDE;
        }
        this.column = columns.get(index);
        return checkColumn(bytes, offset, length);
      }
      
      // ret >= 1
      // Specified column is smaller than current column
      // Nothing to match against, add to new and include
      newColumns.add(new ColumnCount(bytes, offset, length, 1));
      this.newColumn = newColumns.get(newIndex);
      return MatchCode.INCLUDE;
    }

    if (column != null && newColumn != null) {
      // There are new and old, figure which to check first
      int ret = Bytes.compareTo(column.getBuffer(), column.getOffset(), 
        column.getLength(), newColumn.getBuffer(), newColumn.getOffset(), 
        newColumn.getLength());
        
      // Old is smaller than new, compare against old
      if(ret <= -1) {
        ret = Bytes.compareTo(column.getBuffer(), column.getOffset(), 
          column.getLength(), bytes, offset, length);
      
        // Same column
        if(ret == 0) {
          if(column.increment() > this.maxVersions) {
            return MatchCode.SKIP;
          }
          return MatchCode.INCLUDE;
        }
      
        // Specified column is bigger than current column
        // Move down current column and check again
        if(ret <= -1) {
          if(++index == columns.size()) {
            this.column = null;
          } else {
            this.column = columns.get(index);
          }
          return checkColumn(bytes, offset, length);
        }
      
        // ret >= 1
        // Specified column is smaller than current column
        // Nothing to match against, add to new and include
        newColumns.add(new ColumnCount(bytes, offset, length, 1));
        return MatchCode.INCLUDE;
      }
    }

    if (newColumn != null) {
      // Cannot be equal, so ret >= 1
      // New is smaller than old, compare against new
      int ret = Bytes.compareTo(newColumn.getBuffer(), newColumn.getOffset(), 
        newColumn.getLength(), bytes, offset, length);
    
      // Same column
      if(ret == 0) {
        if(newColumn.increment() > this.maxVersions) {
          return MatchCode.SKIP;
        }
        return MatchCode.INCLUDE;
      }
    
      // Specified column is bigger than current column
      // Move down current column and check again
      if(ret <= -1) {
        if(++newIndex == newColumns.size()) {
          this.newColumn = null;
        } else {
          this.newColumn = newColumns.get(newIndex);
        }
        return checkColumn(bytes, offset, length);
      }
    
      // ret >= 1
      // Specified column is smaller than current column
      // Nothing to match against, add to new and include
      newColumns.add(new ColumnCount(bytes, offset, length, 1));
      return MatchCode.INCLUDE;
    }

    // No match happened, add to new and include
    newColumns.add(new ColumnCount(bytes, offset, length, 1));
    return MatchCode.INCLUDE;    
  }
  
  /**
   * Called at the end of every StoreFile or memstore.
   */
  public void update() {
    // If no previous columns, use new columns and return
    if(this.columns == null || this.columns.size() == 0) {
      if(this.newColumns.size() > 0){
        finish(newColumns);
      }
      return;
    }
    
    // If no new columns, retain previous columns and return
    if(this.newColumns.size() == 0) {
      this.index = 0;
      this.column = this.columns.get(index);
      return;
    }
    
    // Merge previous columns with new columns
    // There will be no overlapping
    List<ColumnCount> mergeColumns = new ArrayList<ColumnCount>(
        columns.size() + newColumns.size());
    index = 0;
    newIndex = 0;
    column = columns.get(0);
    newColumn = newColumns.get(0);
    while(true) {
      int ret = Bytes.compareTo(
          column.getBuffer(), column.getOffset(),column.getLength(), 
          newColumn.getBuffer(), newColumn.getOffset(), newColumn.getLength());
      
      // Existing is smaller than new, add existing and iterate it
      if(ret <= -1) {
        mergeColumns.add(column);
        if(++index == columns.size()) {
          // No more existing left, merge down rest of new and return 
          mergeDown(mergeColumns, newColumns, newIndex);
          finish(mergeColumns);
          return;
        }
        column = columns.get(index);
        continue;
      }
      
      // New is smaller than existing, add new and iterate it
      mergeColumns.add(newColumn);
      if(++newIndex == newColumns.size()) {
        // No more new left, merge down rest of existing and return
        mergeDown(mergeColumns, columns, index);
        finish(mergeColumns);
        return;
      }
      newColumn = newColumns.get(newIndex);
      continue;
    }
  }
  
  private void mergeDown(List<ColumnCount> mergeColumns, 
      List<ColumnCount> srcColumns, int srcIndex) {
    int index = srcIndex;
    while(index < srcColumns.size()) {
      mergeColumns.add(srcColumns.get(index++));
    }
  }
  
  private void finish(List<ColumnCount> mergeColumns) {
    this.columns = mergeColumns;
    this.index = 0;
    this.column = this.columns.size() > 0? columns.get(index) : null;
    
    this.newColumns = new ArrayList<ColumnCount>();
    this.newIndex = 0;
    this.newColumn = null;
  }
  
}
