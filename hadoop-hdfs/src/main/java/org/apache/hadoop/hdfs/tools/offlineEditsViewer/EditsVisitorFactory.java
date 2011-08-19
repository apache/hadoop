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

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * EditsVisitorFactory for different implementations of EditsVisitor
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EditsVisitorFactory {

  /**
   * Factory function that creates an EditsVisitor object
   *
   * @param filename output filename
   * @param tokenizer input tokenizer
   * @return EditsVisitor for appropriate output format (binary, XML etc.)
   */
  static public EditsVisitor getEditsVisitor(String filename,
    String processor,
    Tokenizer tokenizer,
    boolean printToScreen) throws IOException {

    if(processor.toLowerCase().equals("xml")) {
      return new XmlEditsVisitor(filename, tokenizer, printToScreen);
    } else if(processor.toLowerCase().equals("stats")) {
      return new StatisticsEditsVisitor(filename, tokenizer, printToScreen);
    } else if(processor.toLowerCase().equals("binary")) {
      return new BinaryEditsVisitor(filename, tokenizer, printToScreen);
    } else {
      throw new IOException("Unknown proccesor " + processor +
        " (valid processors: xml, binary, stats)");
    }
  }
}
