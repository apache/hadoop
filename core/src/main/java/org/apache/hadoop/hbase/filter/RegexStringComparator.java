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
package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.regex.Pattern;

/**
 * This comparator is for use with {@link CompareFilter} implementations, such
 * as {@link RowFilter}, {@link QualifierFilter}, and {@link ValueFilter}, for
 * filtering based on the value of a given column. Use it to test if a given
 * regular expression matches a cell value in the column.
 * <p>
 * Only EQUAL or NOT_EQUAL comparisons are valid with this comparator.
 * <p>
 * For example:
 * <p>
 * <pre>
 * ValueFilter vf = new ValueFilter(CompareOp.EQUAL,
 *     new RegexStringComparator(
 *       // v4 IP address
 *       "(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3,3}" +
 *         "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))(\\/[0-9]+)?" +
 *         "|" +
 *       // v6 IP address
 *       "((([\\dA-Fa-f]{1,4}:){7}[\\dA-Fa-f]{1,4})(:([\\d]{1,3}.)" +
 *         "{3}[\\d]{1,3})?)(\\/[0-9]+)?"));
 * </pre>
 */
public class RegexStringComparator extends WritableByteArrayComparable {

  private static final Log LOG = LogFactory.getLog(RegexStringComparator.class);

  private Charset charset = Charset.forName(HConstants.UTF8_ENCODING);

  private Pattern pattern;

  /** Nullary constructor for Writable, do not use */
  public RegexStringComparator() { }

  /**
   * Constructor
   * @param expr a valid regular expression
   */
  public RegexStringComparator(String expr) {
    super(Bytes.toBytes(expr));
    this.pattern = Pattern.compile(expr, Pattern.DOTALL);
  }

  /**
   * Specifies the {@link Charset} to use to convert the row key to a String.
   * <p>
   * The row key needs to be converted to a String in order to be matched
   * against the regular expression.  This method controls which charset is
   * used to do this conversion.
   * <p>
   * If the row key is made of arbitrary bytes, the charset {@code ISO-8859-1}
   * is recommended.
   * @param charset The charset to use.
   */
  public void setCharset(final Charset charset) {
    this.charset = charset;
  }

  @Override
  public int compareTo(byte[] value) {
    // Use find() for subsequence match instead of matches() (full sequence
    // match) to adhere to the principle of least surprise.
    return pattern.matcher(new String(value, charset)).find() ? 0 : 1;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    final String expr = in.readUTF();
    this.value = Bytes.toBytes(expr);
    this.pattern = Pattern.compile(expr);
    final String charset = in.readUTF();
    if (charset.length() > 0) {
      try {
        this.charset = Charset.forName(charset);
      } catch (IllegalCharsetNameException e) {
        LOG.error("invalid charset", e);
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(pattern.toString());
    out.writeUTF(charset.name());
  }

}
