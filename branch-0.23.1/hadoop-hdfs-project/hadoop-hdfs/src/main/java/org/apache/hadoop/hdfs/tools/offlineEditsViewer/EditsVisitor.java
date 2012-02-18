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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.ByteToken;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.ShortToken;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.IntToken;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.VIntToken;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.LongToken;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.VLongToken;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.StringUTF8Token;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.StringTextToken;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.BlobToken;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.BytesWritableToken;
import static org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer.EmptyToken;

/**
 * An implementation of EditsVisitor can traverse the structure of an
 * Hadoop edits log and respond to each of the structures within the file.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
abstract public class EditsVisitor {

  private Tokenizer tokenizer;

  public EditsVisitor(Tokenizer tokenizer) {
    this.tokenizer = tokenizer;
  }

  /**
   * Begin visiting the edits log structure.  Opportunity to perform
   * any initialization necessary for the implementing visitor.
   */
  abstract void start() throws IOException;

  /**
   * Finish visiting the edits log structure.  Opportunity to perform any
   * clean up necessary for the implementing visitor.
   */
  abstract void finish() throws IOException;

  /**
   * Finish visiting the edits log structure after an error has occurred
   * during the processing.  Opportunity to perform any clean up necessary
   * for the implementing visitor.
   */
  abstract void finishAbnormally() throws IOException;

  /**
   * Visit non enclosing element of edits log with specified value.
   *
   * @param value a token to visit
   */
  abstract Tokenizer.Token visit(Tokenizer.Token value) throws IOException;

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public ByteToken visitByte(EditsElement e) throws IOException {
    return (ByteToken)visit(tokenizer.read(new ByteToken(e)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public ShortToken visitShort(EditsElement e) throws IOException {
    return (ShortToken)visit(tokenizer.read(new ShortToken(e)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public IntToken visitInt(EditsElement e) throws IOException {
    return (IntToken)visit(tokenizer.read(new IntToken(e)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public VIntToken visitVInt(EditsElement e) throws IOException {
    return (VIntToken)visit(tokenizer.read(new VIntToken(e)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public LongToken visitLong(EditsElement e) throws IOException {
    return (LongToken)visit(tokenizer.read(new LongToken(e)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public VLongToken visitVLong(EditsElement e) throws IOException {
    return (VLongToken)visit(tokenizer.read(new VLongToken(e)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public StringUTF8Token visitStringUTF8(EditsElement e) throws IOException {
    return (StringUTF8Token)visit(tokenizer.read(new StringUTF8Token(e)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public StringTextToken visitStringText(EditsElement e) throws IOException {
    return (StringTextToken)visit(tokenizer.read(new StringTextToken(e)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public BlobToken visitBlob(EditsElement e, int length) throws IOException {
    return (BlobToken)visit(tokenizer.read(new BlobToken(e, length)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public BytesWritableToken visitBytesWritable(EditsElement e) throws IOException {
    return (BytesWritableToken)visit(tokenizer.read(new BytesWritableToken(e)));
  }

  /**
   * Convenience shortcut method to parse a specific token type
   */
  public EmptyToken visitEmpty(EditsElement e) throws IOException {
    return (EmptyToken)visit(tokenizer.read(new EmptyToken(e)));
  }

  /**
   * Begin visiting an element that encloses another element, such as
   * the beginning of the list of blocks that comprise a file.
   *
   * @param value Token being visited
   */
  abstract void visitEnclosingElement(Tokenizer.Token value)
     throws IOException;

  /**
   * Convenience shortcut method (it virutally always uses EmptyToken)
   */
  void visitEnclosingElement(EditsElement e) throws IOException {
    visitEnclosingElement(tokenizer.read(new EmptyToken(e)));
  }

  /**
   * Leave current enclosing element.  Called, for instance, at the end of
   * processing the blocks that compromise a file.
   */
  abstract void leaveEnclosingElement() throws IOException;
}
