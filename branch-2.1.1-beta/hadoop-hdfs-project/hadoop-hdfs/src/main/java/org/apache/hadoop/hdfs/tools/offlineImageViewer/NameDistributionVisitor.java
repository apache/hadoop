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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * File name distribution visitor. 
 * <p>
 * It analyzes file names in fsimage and prints the following information: 
 * <li>Number of unique file names</li> 
 * <li>Number file names and the corresponding number range of files that use 
 * these same names</li>
 * <li>Heap saved if the file name objects are reused</li>
 */
@InterfaceAudience.Private
public class NameDistributionVisitor extends TextWriterImageVisitor {
  HashMap<String, Integer> counts = new HashMap<String, Integer>();

  public NameDistributionVisitor(String filename, boolean printToScreen)
      throws IOException {
    super(filename, printToScreen);
  }

  @Override
  void finish() throws IOException {
    final int BYTEARRAY_OVERHEAD = 24;

    write("Total unique file names " + counts.size());
    // Columns: Frequency of file occurrence, savings in heap, total files using
    // the name and number of file names
    final long stats[][] = { { 100000, 0, 0, 0 },
                             { 10000, 0, 0, 0 },
                             { 1000, 0, 0, 0 },
                             { 100, 0, 0, 0 },
                             { 10, 0, 0, 0 },
                             { 5, 0, 0, 0 },
                             { 4, 0, 0, 0 },
                             { 3, 0, 0, 0 },
                             { 2, 0, 0, 0 }};

    int highbound = Integer.MIN_VALUE;
    for (Entry<String, Integer> entry : counts.entrySet()) {
      highbound = Math.max(highbound, entry.getValue());
      for (int i = 0; i < stats.length; i++) {
        if (entry.getValue() >= stats[i][0]) {
          stats[i][1] += (BYTEARRAY_OVERHEAD + entry.getKey().length())
              * (entry.getValue() - 1);
          stats[i][2] += entry.getValue();
          stats[i][3]++;
          break;
        }
      }
    }

    long lowbound = 0;
    long totalsavings = 0;
    for (long[] stat : stats) {
      lowbound = stat[0];
      totalsavings += stat[1];
      String range = lowbound == highbound ? " " + lowbound :
          " between " + lowbound + "-" + highbound;
      write("\n" + stat[3] + " names are used by " + stat[2] + " files"
          + range + " times. Heap savings ~" + stat[1] + " bytes.");
      highbound = (int) stat[0] - 1;
    }
    write("\n\nTotal saved heap ~" + totalsavings + "bytes.\n");
    super.finish();
  }

  @Override
  void visit(ImageElement element, String value) throws IOException {
    if (element == ImageElement.INODE_PATH) {
      String filename = value.substring(value.lastIndexOf("/") + 1);
      if (counts.containsKey(filename)) {
        counts.put(filename, counts.get(filename) + 1);
      } else {
        counts.put(filename, 1);
      }
    }
  }

  @Override
  void leaveEnclosingElement() throws IOException {
  }

  @Override
  void start() throws IOException {
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
  }

  @Override
  void visitEnclosingElement(ImageElement element, ImageElement key,
      String value) throws IOException {
  }
}
