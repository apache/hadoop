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
import java.util.LinkedList;

import org.apache.hadoop.hdfs.tools.offlineImageViewer.DepthCounter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An XmlEditsVisitor walks over an EditLog structure and writes out
 * an equivalent XML document that contains the EditLog's components.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XmlEditsVisitor extends TextEditsVisitor {
  final private LinkedList<EditsElement> tagQ =
    new LinkedList<EditsElement>();

  final private DepthCounter depthCounter = new DepthCounter();

  /**
   * Create a processor that writes to the file named and may or may not
   * also output to the screen, as specified.
   *
   * @param filename Name of file to write output to
   * @param tokenizer Input tokenizer
   */
  public XmlEditsVisitor(String filename, Tokenizer tokenizer)
    throws IOException {

    super(filename, tokenizer, false);
  }

  /**
   * Create a processor that writes to the file named and may or may not
   * also output to the screen, as specified.
   *
   * @param filename Name of file to write output to
   * @param tokenizer Input tokenizer
   * @param printToScreen Mirror output to screen? (ignored for binary)
   */
  public XmlEditsVisitor(String filename,
    Tokenizer tokenizer,
    boolean printToScreen) throws IOException {

    super(filename, tokenizer, printToScreen);
  }

  /**
   * Start visitor (initialization)
   */
  @Override
  void start() throws IOException {
    write("<?xml version=\"1.0\"?>\n");
  }

  /**
   * Finish visitor
   */
  @Override
  void finish() throws IOException {
    super.finish();
  }

  /**
   * Finish with error
   */
  @Override
  void finishAbnormally() throws IOException {
    write("\n<!-- Error processing EditLog file.  Exiting -->\n");
    super.finishAbnormally();
  }

  /**
   * Visit a Token
   *
   * @param value a Token to visit
   */
  @Override
  Tokenizer.Token visit(Tokenizer.Token value) throws IOException {
    writeTag(value.getEditsElement().toString(), value.toString());
    return value;
  }

  /**
   * Visit an enclosing element (element that cntains other elements)
   *
   * @param value a Token to visit
   */
  @Override
  void visitEnclosingElement(Tokenizer.Token value) throws IOException {
    printIndents();
    write("<" + value.getEditsElement().toString() + ">\n");
    tagQ.push(value.getEditsElement());
    depthCounter.incLevel();
  }

  /**
   * Leave enclosing element
   */
  @Override
  void leaveEnclosingElement() throws IOException {
    depthCounter.decLevel();
    if(tagQ.size() == 0)
      throw new IOException("Tried to exit non-existent enclosing element " +
                "in EditLog file");

    EditsElement element = tagQ.pop();
    printIndents();
    write("</" + element.toString() + ">\n");
  }

  /**
   * Write an XML tag
   *
   * @param tag a tag name
   * @param value a tag value
   */
  private void writeTag(String tag, String value) throws IOException {
    printIndents();
    if(value.length() > 0) {
      write("<" + tag + ">" + value + "</" + tag + ">\n");
    } else {
      write("<" + tag + "/>\n");
    }
  }

  // prepared values that printIndents is likely to use
  final private static String [] indents = {
     "",
     "  ",
     "    ",
     "      ",
     "        ",
     "          ",
     "            " };

  /**
   * Prints the leading spaces based on depth level
   */
  private void printIndents() throws IOException {
    try {
      write(indents[depthCounter.getLevel()]);
    } catch (IndexOutOfBoundsException e) {
      // unlikely needed so can be slow
      for(int i = 0; i < depthCounter.getLevel(); i++)
        write("  ");
    }
   
  }
}
