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
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.io.Text;

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
    private Text family;
    private Pattern columnMatcher;
    private Text col;
  
    ColumnMatcher(final Text col) throws IOException {
      Text qualifier = HStoreKey.extractQualifier(col);
      try {
        if (qualifier == null || qualifier.getLength() == 0) {
          this.matchType = MATCH_TYPE.FAMILY_ONLY;
          this.family = HStoreKey.extractFamily(col).toText();
          this.wildCardmatch = true;
        } else if(isRegexPattern.matcher(qualifier.toString()).matches()) {
          this.matchType = MATCH_TYPE.REGEX;
          this.columnMatcher = Pattern.compile(col.toString());
          this.wildCardmatch = true;
        } else {
          this.matchType = MATCH_TYPE.SIMPLE;
          this.col = col;
          this.wildCardmatch = false;
        }
      } catch(Exception e) {
        throw new IOException("Column: " + col + ": " + e.getMessage());
      }
    }
    
    /** Matching method */
    boolean matches(Text c) throws IOException {
      if(this.matchType == MATCH_TYPE.SIMPLE) {
        return c.equals(this.col);
      } else if(this.matchType == MATCH_TYPE.FAMILY_ONLY) {
        return HStoreKey.extractFamily(c).equals(this.family);
      } else if(this.matchType == MATCH_TYPE.REGEX) {
        return this.columnMatcher.matcher(c.toString()).matches();
      } else {
        throw new IOException("Invalid match type: " + this.matchType);
      }
    }
    
    boolean isWildCardMatch() {
      return this.wildCardmatch;
    }
  }

  // Holds matchers for each column family 
  protected TreeMap<Text, Vector<ColumnMatcher>> okCols;
  
  // True when scanning is done
  protected volatile boolean scannerClosed = false;
  
  // The timestamp to match entries against
  protected long timestamp;
  
  private boolean wildcardMatch;
  private boolean multipleMatchers;

  /** Constructor for abstract base class */
  HAbstractScanner(long timestamp, Text[] targetCols) throws IOException {
    this.timestamp = timestamp;
    this.wildcardMatch = false;
    this.multipleMatchers = false;
    this.okCols = new TreeMap<Text, Vector<ColumnMatcher>>();
    for(int i = 0; i < targetCols.length; i++) {
      Text family = HStoreKey.extractFamily(targetCols[i]).toText();
      Vector<ColumnMatcher> matchers = okCols.get(family);
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
      okCols.put(family, matchers);
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
  protected boolean columnMatch(final Text column) throws IOException {
    Vector<ColumnMatcher> matchers =
      this.okCols.get(HStoreKey.extractFamily(column));
    if (matchers == null) {
      return false;
    }
    for(int m = 0; m < matchers.size(); m++) {
      if(matchers.get(m).matches(column)) {
        return true;
      }
    }
    return false;
  }

  /** {@inheritDoc} */
  public boolean isWildcardScanner() {
    return this.wildcardMatch;
  }
  
  /** {@inheritDoc} */
  public boolean isMultipleMatchScanner() {
    return this.multipleMatchers;
  }

  public abstract boolean next(HStoreKey key, SortedMap<Text, byte []> results)
  throws IOException;
  
  public Iterator<Entry<HStoreKey, SortedMap<Text, byte[]>>> iterator() {
    throw new UnsupportedOperationException("Unimplemented serverside. " +
      "next(HStoreKey, StortedMap(...) is more efficient");
  }
}
