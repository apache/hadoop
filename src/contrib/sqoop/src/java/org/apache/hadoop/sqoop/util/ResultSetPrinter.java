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

package org.apache.hadoop.sqoop.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility methods to format and print ResultSet objects
 * 
 *
 */
public class ResultSetPrinter {

  public static final Log LOG = LogFactory.getLog(ResultSetPrinter.class.getName());

  // max output width to allocate to any column of the printed results.
  private static final int MAX_COL_WIDTH = 20;

  // length of the byte buffer, in bytes, to allocate.
  private static final int BUFFER_LEN = 4096;

  // maximum number of characters to deserialize from the stringbuilder
  // into the byte buffer at a time. Factor of 2 off b/c of Unicode.
  private static final int MAX_CHARS = 2048;

  private ByteBuffer bytebuf;
  private char [] charArray;

  public ResultSetPrinter() {
    bytebuf = ByteBuffer.allocate(BUFFER_LEN);
    charArray = new char[MAX_CHARS];
  }

  /**
   * Print 'str' to the string builder, padded to 'width' chars
   */
  private static void printPadded(StringBuilder sb, String str, int width) {
    int numPad;
    if (null == str) {
      sb.append("(null)");
      numPad = width - "(null)".length();
    } else {
      sb.append(str);
      numPad = width - str.length();
    }

    for (int i = 0; i < numPad; i++) {
      sb.append(' ');
    }
  }


  /**
   * Takes the contents of the StringBuilder and prints it on the OutputStream
   */
  private void sendToStream(StringBuilder sb, OutputStream os) throws IOException {

    int pos = 0;  // current pos in the string builder
    int len = sb.length(); // total length (in characters) to send to os.
    CharBuffer charbuf = bytebuf.asCharBuffer();

    while (pos < len) {
      int copyLen = Math.min(sb.length(), MAX_CHARS);
      sb.getChars(pos, copyLen, charArray, 0);

      charbuf.put(charArray, 0, copyLen);
      os.write(bytebuf.array());

      pos += copyLen;
    }

  }

  private static final String COL_SEPARATOR = " | ";

  /**
   * Format the contents of the ResultSet into something that could be printed
   * neatly; the results are appended to the supplied StringBuilder.
   */
  public final void printResultSet(OutputStream os, ResultSet results) throws IOException {
    try {
      StringBuilder sbNames = new StringBuilder();
      int cols = results.getMetaData().getColumnCount();

      int [] colWidths = new int[cols];
      ResultSetMetaData metadata = results.getMetaData();
      for (int i = 1; i < cols + 1; i++) {
        String colName = metadata.getColumnName(i);
        colWidths[i - 1] = Math.min(metadata.getColumnDisplaySize(i), MAX_COL_WIDTH);
        if (colName == null || colName.equals("")) {
          colName = metadata.getColumnLabel(i) + "*";
        }
        printPadded(sbNames, colName, colWidths[i - 1]);
        sbNames.append(COL_SEPARATOR);
      }
      sbNames.append('\n');

      StringBuilder sbPad = new StringBuilder();
      for (int i = 0; i < cols; i++) {
        for (int j = 0; j < COL_SEPARATOR.length() + colWidths[i]; j++) {
          sbPad.append('-');
        }
      }
      sbPad.append('\n');

      sendToStream(sbPad, os);
      sendToStream(sbNames, os);
      sendToStream(sbPad, os);

      while (results.next())  {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < cols + 1; i++) {
          printPadded(sb, results.getString(i), colWidths[i - 1]);
          sb.append(COL_SEPARATOR);
        }
        sb.append('\n');
        sendToStream(sb, os);
      }

      sendToStream(sbPad, os);
    } catch (SQLException sqlException) {
      LOG.error("Error reading from database: " + sqlException.toString());
    }
  }

}

