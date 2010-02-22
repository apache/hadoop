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
import java.util.List;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.regionserver.QueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is used for the tracking and enforcement of columns and numbers 
 * of versions during the course of a Get or Scan operation, when explicit
 * column qualifiers have been asked for in the query.
 *
 * With a little magic (see {@link ScanQueryMatcher}), we can use this matcher
 * for both scans and gets.  The main difference is 'next' and 'done' collapse
 * for the scan case (since we see all columns in order), and we only reset
 * between rows.
 * 
 * <p>
 * This class is utilized by {@link QueryMatcher} through two methods:
 * <ul><li>{@link #checkColumn} is called when a Put satisfies all other
 * conditions of the query.  This method returns a {@link MatchCode} to define
 * what action should be taken.
 * <li>{@link #update} is called at the end of every StoreFile or memstore.
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded 
 */
public class ExplicitColumnTracker implements ColumnTracker {

  private int maxVersions;
  private List<ColumnCount> columns;
  private int index;
  private ColumnCount column;
  private NavigableSet<byte[]> origColumns;
  
  /**
   * Default constructor.
   * @param columns columns specified user in query
   * @param maxVersions maximum versions to return per column
   */
  public ExplicitColumnTracker(NavigableSet<byte[]> columns, int maxVersions) {
    this.maxVersions = maxVersions;
    this.origColumns = columns;
    reset();
  }
  
  /**
   * Done when there are no more columns to match against.
   */
  public boolean done() {
    return this.columns.size() == 0;
  }

  public ColumnCount getColumnHint() {
    return this.column;
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
    // No more columns left, we are done with this query
    if(this.columns.size() == 0) {
      return MatchCode.DONE; // done_row
    }
    
    // No more columns to match against, done with storefile
    if(this.column == null) {
      return MatchCode.NEXT; // done_row
    }
    
    // Compare specific column to current column
    int ret = Bytes.compareTo(column.getBuffer(), column.getOffset(), 
        column.getLength(), bytes, offset, length);
    
    // Matches, decrement versions left and include
    if(ret == 0) {
      if(this.column.decrement() == 0) {
        // Done with versions for this column
        this.columns.remove(this.index);
        if(this.columns.size() == this.index) {
          // Will not hit any more columns in this storefile
          this.column = null;
        } else {
          this.column = this.columns.get(this.index);
        }
      }
      return MatchCode.INCLUDE;
    }

    // Specified column is bigger than current column
    // Move down current column and check again
    if(ret <= -1) {
      if(++this.index == this.columns.size()) {
        // No more to match, do not include, done with storefile
        return MatchCode.NEXT; // done_row
      }
      this.column = this.columns.get(this.index);
      return checkColumn(bytes, offset, length);
    }

    // Specified column is smaller than current column
    // Skip
    return MatchCode.SKIP; // skip to next column, with hint?
  }
  
  /**
   * Called at the end of every StoreFile or memstore.
   */
  public void update() {
    if(this.columns.size() != 0) {
      this.index = 0;
      this.column = this.columns.get(this.index);
    } else {
      this.index = -1;
      this.column = null;
    }
  }

  // Called between every row.
  public void reset() {
    buildColumnList(this.origColumns);
    this.index = 0;
    this.column = this.columns.get(this.index);
  }

  private void buildColumnList(NavigableSet<byte[]> columns) {
    this.columns = new ArrayList<ColumnCount>(columns.size());
    for(byte [] column : columns) {
      this.columns.add(new ColumnCount(column,maxVersions));
    }
  }
}
