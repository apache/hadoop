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
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ColumnNameParseException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Abstract base class that implements the InternalScanner.
 */
public abstract class HAbstractScanner implements InternalScanner {
  final Log LOG = LogFactory.getLog(this.getClass().getName());

  // Pattern to determine if a column key is a regex
  static final Pattern isRegexPattern =
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

  private final List<ColumnMatcher> matchers = new ArrayList<ColumnMatcher>();

  // True when scanning is done
  protected volatile boolean scannerClosed = false;

  // The timestamp to match entries against
  protected final long timestamp;

  private boolean wildcardMatch = false;
  private boolean multipleMatchers = false;

  /** Constructor for abstract base class */
  protected HAbstractScanner(final long timestamp,
    final NavigableSet<byte []> columns)
  throws IOException {
    this.timestamp = timestamp;
    for (byte [] column: columns) {
      ColumnMatcher matcher = new ColumnMatcher(column);
      this.wildcardMatch = matcher.isWildCardMatch();
      matchers.add(matcher);
      this.multipleMatchers = !matchers.isEmpty();
    }
  }

  /**
   * For a particular column, find all the matchers defined for the column.
   * Compare the column family and column key using the matchers. The first one
   * that matches returns true. If no matchers are successful, return false.
   * 
   * @param kv KeyValue to test
   * @return true if any of the matchers for the column match the column family
   * and the column key.
   *                 
   * @throws IOException
   */
  protected boolean columnMatch(final KeyValue kv)
  throws IOException {
    if (matchers == null) {
      return false;
    }
    for(int m = 0; m < this.matchers.size(); m++) {
      if (this.matchers.get(m).matches(kv)) {
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

  public abstract boolean next(List<KeyValue> results)
  throws IOException;

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
    // Column without delimiter so easy compare to KeyValue column
    private byte [] col;
  
    ColumnMatcher(final byte [] col) throws IOException {
      byte [][] parse = parseColumn(col);
      // Make up column without delimiter
      byte [] columnWithoutDelimiter =
        new byte [parse[0].length + parse[1].length];
      System.arraycopy(parse[0], 0, columnWithoutDelimiter, 0, parse[0].length);
      System.arraycopy(parse[1], 0, columnWithoutDelimiter, parse[0].length,
        parse[1].length);
      // First position has family.  Second has qualifier.
      byte [] qualifier = parse[1];
      try {
        if (qualifier == null || qualifier.length == 0) {
          this.matchType = MATCH_TYPE.FAMILY_ONLY;
          this.family = parse[0];
          this.wildCardmatch = true;
        } else if (isRegexPattern.matcher(Bytes.toString(qualifier)).matches()) {
          this.matchType = MATCH_TYPE.REGEX;
          this.columnMatcher = Pattern.compile(Bytes.toString(columnWithoutDelimiter));
          this.wildCardmatch = true;
        } else {
          this.matchType = MATCH_TYPE.SIMPLE;
          this.col = columnWithoutDelimiter;
          this.wildCardmatch = false;
        }
      } catch(Exception e) {
        throw new IOException("Column: " + Bytes.toString(col) + ": " +
          e.getMessage());
      }
    }
    
    /**
     * @param kv
     * @return
     * @throws IOException
     */
    boolean matches(final KeyValue kv) throws IOException {
      if (this.matchType == MATCH_TYPE.SIMPLE) {
        return kv.matchingColumnNoDelimiter(this.col);
      } else if(this.matchType == MATCH_TYPE.FAMILY_ONLY) {
        return kv.matchingFamily(this.family);
      } else if (this.matchType == MATCH_TYPE.REGEX) {
        // Pass a column without the delimiter since thats whats we're
        // expected to match.
        int o = kv.getColumnOffset();
        int l = kv.getColumnLength(o);
        String columnMinusQualifier = Bytes.toString(kv.getBuffer(), o, l);
        return this.columnMatcher.matcher(columnMinusQualifier).matches();
      } else {
        throw new IOException("Invalid match type: " + this.matchType);
      }
    }

    boolean isWildCardMatch() {
      return this.wildCardmatch;
    }

    /**
     * @param c Column name
     * @return Return array of size two whose first element has the family
     * prefix of passed column <code>c</code> and whose second element is the
     * column qualifier.
     * @throws ColumnNameParseException 
     */
    public static byte [][] parseColumn(final byte [] c)
    throws ColumnNameParseException {
      final byte [][] result = new byte [2][];
      // TODO: Change this so don't do parse but instead use the comparator
      // inside in KeyValue which just looks at column family.
      final int index = KeyValue.getFamilyDelimiterIndex(c, 0, c.length);
      if (index == -1) {
        throw new ColumnNameParseException("Impossible column name: " + c);
      }
      result[0] = new byte [index];
      System.arraycopy(c, 0, result[0], 0, index);
      final int len = c.length - (index + 1);
      result[1] = new byte[len];
      System.arraycopy(c, index + 1 /*Skip delimiter*/, result[1], 0,
        len);
      return result;
    }
  }
}