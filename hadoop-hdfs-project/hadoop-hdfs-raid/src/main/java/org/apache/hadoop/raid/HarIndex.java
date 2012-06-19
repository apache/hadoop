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

package org.apache.hadoop.raid;

import java.io.InputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.LinkedList;

import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.io.Text;

/**
 * Represents the contents of a HAR Index file. The HAR is assumed to be
 * comprising of RAID parity files only and no directories.
 */
public class HarIndex {
  public static final String indexFileName = "_index";
  private List<IndexEntry> entries = new LinkedList<IndexEntry>();

  /**
   * Represents information in a single line of the HAR index file.
   */
  public static class IndexEntry {
    String fileName; // Name of the file in the part file.
    long startOffset; // Start offset within the part file.
    long length; // Length of this file within the part file.
    long mtime; // Modification time of the file.
    String partFileName; // Name of the part file.

    IndexEntry(String fileName, long startOffset, long length,
                long mtime, String partFileName) {
      this.fileName = fileName;
      this.startOffset = startOffset;
      this.length = length;
      this.mtime = mtime;
      this.partFileName = partFileName;
    }

    public String toString() {
      return "fileName=" + fileName +
             ", startOffset=" + startOffset +
             ", length=" + length +
             ", mtime=" + mtime +
             ", partFileName=" + partFileName;
    }
  }

  /**
   * Constructor that reads the contents of the index file.
   * @param in An input stream to the index file.
   * @param max The size of the index file.
   * @throws IOException
   */
  public HarIndex(InputStream in, long max) throws IOException {
    LineReader lineReader = new LineReader(in);
    Text text = new Text();
    long nread = 0;
    while (nread < max) {
      int n = lineReader.readLine(text);
      nread += n;
      String line = text.toString();
      try {
        parseLine(line);
      } catch (UnsupportedEncodingException e) {
        throw new IOException("UnsupportedEncodingException after reading " +
                              nread + "bytes");
      }
    }
  }

  /**
   * Parses each line and extracts relevant information.
   * @param line
   * @throws UnsupportedEncodingException
   */
  void parseLine(String line) throws UnsupportedEncodingException {
    String[] splits = line.split(" ");

    boolean isDir = "dir".equals(splits[1]) ? true: false;
    if (!isDir && splits.length >= 6) {
      String name = URLDecoder.decode(splits[0], "UTF-8");
      String partName = URLDecoder.decode(splits[2], "UTF-8");
      long startIndex = Long.parseLong(splits[3]);
      long length = Long.parseLong(splits[4]);
      String[] newsplits = URLDecoder.decode(splits[5],"UTF-8").split(" ");
      if (newsplits != null && newsplits.length >= 4) {
        long mtime = Long.parseLong(newsplits[0]);
        IndexEntry entry = new IndexEntry(
          name, startIndex, length, mtime, partName);
        entries.add(entry);
      }
    }
  }

  /**
   * Finds the index entry corresponding to a HAR partFile at an offset.
   * @param partName The name of the part file (part-*).
   * @param partFileOffset The offset into the part file.
   * @return The entry corresponding to partName:partFileOffset.
   */
  public IndexEntry findEntry(String partName, long partFileOffset) {
    for (IndexEntry e: entries) {
      boolean nameMatch = partName.equals(e.partFileName);
      boolean inRange = (partFileOffset >= e.startOffset) &&
                        (partFileOffset < e.startOffset + e.length);
      if (nameMatch && inRange) {
        return e;
      }
    }
    return null;
  }

  /**
   * Finds the index entry corresponding to a file in the archive
   */
  public IndexEntry findEntryByFileName(String fileName) {
    for (IndexEntry e: entries) {
      if (fileName.equals(e.fileName)) {
        return e;
      }
    }
    return null;
  }

}
