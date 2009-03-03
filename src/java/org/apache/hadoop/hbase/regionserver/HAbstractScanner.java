/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Abstract base class that implements the InternalScanner.
 */
public abstract class HAbstractScanner implements InternalScanner {
  final Log LOG = LogFactory.getLog(this.getClass().getName());

  // Pattern to determine if a column key is a regex
  static Pattern isRegexPattern =
    Pattern.compile("^.*[\\\\+|^&*$\\[\\]\\}{)(]+.*$");
  
  /** The kind of match we are doing on a column: */
  private static enum MATCH_TYPE {
    /** Just check the column family name */
    FAMILY_ONLY,
    /** Column family + matches regex */
    REGEX,
    /** Literal matching */
    SIMPLE
  }

  /**
   * This class provides column matching functions that are more sophisticated
   * than a simple string compare. There are three types of matching:
   * <ol>
   * <li>Match on the column family name only</li>
   * <li>Match on the column family + column key regex</li>
   * <li>Simple match: compare column family + column key literally</li>
   * </ul>
   */
  private static class ColumnMatcher {
    private boolean wildCardmatch;
    private MATCH_TYPE matchType;
    private byte [] family;
    private Pattern columnMatcher;
    private byte [] col;
  
    ColumnMatcher(final byte [] col) throws IOException {
      byte [][] parse = HStoreKey.parseColumn(col);
      // First position has family.  Second has qualifier.
      byte [] qualifier = parse[1];
      try {
        if (qualifier == null || qualifier.length == 0) {
          this.matchType = MATCH_TYPE.FAMILY_ONLY;
          this.family = parse[0];
          this.wildCardmatch = true;
        } else if (isRegexPattern.matcher(Bytes.toString(qualifier)).matches()) {
          this.matchType = MATCH_TYPE.REGEX;
          this.columnMatcher = Pattern.compile(Bytes.toString(col));
          this.wildCardmatch = true;
        } else {
          this.matchType = MATCH_TYPE.SIMPLE;
          this.col = col;
          this.wildCardmatch = false;
        }
      } catch(Exception e) {
        throw new IOException("Column: " + Bytes.toString(col) + ": " +
          e.getMessage());
      }
    }
    
    /** Matching method */
    boolean matches(final byte [] c) throws IOException {
      if(this.matchType == MATCH_TYPE.SIMPLE) {
        return Bytes.equals(c, this.col);
      } else if(this.matchType == MATCH_TYPE.FAMILY_ONLY) {
        return HStoreKey.matchingFamily(this.family, c);
      } else if (this.matchType == MATCH_TYPE.REGEX) {
        return this.columnMatcher.matcher(Bytes.toString(c)).matches();
      } else {
        throw new IOException("Invalid match type: " + this.matchType);
      }
    }
    
    boolean isWildCardMatch() {
      return this.wildCardmatch;
    }
  }

  // Holds matchers for each column family.  Its keyed by the byte [] hashcode
  // which you can get by calling Bytes.mapKey.
  private Map<Integer, Vector<ColumnMatcher>> okCols =
    new HashMap<Integer, Vector<ColumnMatcher>>();
  
  // True when scanning is done
  protected volatile boolean scannerClosed = false;
  
  // The timestamp to match entries against
  protected long timestamp;
  
  private boolean wildcardMatch;
  private boolean multipleMatchers;

  /** Constructor for abstract base class */
  protected HAbstractScanner(long timestamp, byte [][] targetCols)
  throws IOException {
    this.timestamp = timestamp;
    this.wildcardMatch = false;
    this.multipleMatchers = false;
    for(int i = 0; i < targetCols.length; i++) {
      Integer key = HStoreKey.getFamilyMapKey(targetCols[i]);
      Vector<ColumnMatcher> matchers = okCols.get(key);
      if (matchers == null) {
        matchers = new Vector<ColumnMatcher>();
      }
      ColumnMatcher matcher = new ColumnMatcher(targetCols[i]);
      if (matcher.isWildCardMatch()) {
        this.wildcardMatch = true;
      }
      matchers.add(matcher);
      if (matchers.size() > 1) {
        this.multipleMatchers = true;
      }
      okCols.put(key, matchers);
    }
  }

  /**
   * For a particular column, find all the matchers defined for the column.
   * Compare the column family and column key using the matchers. The first one
   * that matches returns true. If no matchers are successful, return false.
   * 
   * @param column Column to test
   * @return true if any of the matchers for the column match the column family
   * and the column key.
   *                 
   * @throws IOException
   */
  protected boolean columnMatch(final byte [] column) throws IOException {
    Vector<ColumnMatcher> matchers =
      this.okCols.get(HStoreKey.getFamilyMapKey(column));
    if (matchers == null) {
      return false;
    }
    for(int m = 0; m < matchers.size(); m++) {
      if (matchers.get(m).matches(column)) {
        return true;
      }
    }
    return false;
  }

  public boolean isWildcardScanner() {
    return this.wildcardMatch;
  }
  
  public boolean isMultipleMatchScanner() {
    return this.multipleMatchers;
  }

  public abstract boolean next(HStoreKey key, SortedMap<byte [], Cell> results)
  throws IOException;
  
}
