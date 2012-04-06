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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;

/**
 * EditsVisitorFactory for different implementations of EditsVisitor
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OfflineEditsVisitorFactory {
  /**
   * Factory function that creates an EditsVisitor object
   *
   * @param filename              output filename
   * @param processor             type of visitor to create 
   * @param printToScreen         parameter passed to visitor constructor
   *
   * @return EditsVisitor for appropriate output format (binary, xml, etc.)
   */
  static public OfflineEditsVisitor getEditsVisitor(String filename,
    String processor, boolean printToScreen) throws IOException {
    if(processor.toLowerCase().equals("binary")) {
      return new BinaryEditsVisitor(filename);
    }
    OfflineEditsVisitor vis;
    OutputStream fout = new FileOutputStream(filename);
    OutputStream out = null;
    try {
      if (!printToScreen) {
        out = fout;
      }
      else {
        OutputStream outs[] = new OutputStream[2];
        outs[0] = fout;
        outs[1] = System.out;
        out = new TeeOutputStream(outs);
      }
      if(processor.toLowerCase().equals("xml")) {
        vis = new XmlEditsVisitor(out);
      } else if(processor.toLowerCase().equals("stats")) {
        vis = new StatisticsEditsVisitor(out);
      } else {
        throw new IOException("Unknown proccesor " + processor +
          " (valid processors: xml, binary, stats)");
      }
      out = fout = null;
      return vis;
    } finally {
      IOUtils.closeStream(fout);
      IOUtils.closeStream(out);
    }
  }
}
