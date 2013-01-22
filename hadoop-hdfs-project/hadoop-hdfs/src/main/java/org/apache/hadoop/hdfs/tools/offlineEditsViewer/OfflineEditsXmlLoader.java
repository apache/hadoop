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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Stack;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.OpInstanceCache;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsViewer;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import com.google.common.base.Charsets;

/**
 * OfflineEditsXmlLoader walks an EditsVisitor over an OEV XML file
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OfflineEditsXmlLoader 
    extends DefaultHandler implements OfflineEditsLoader {
  private final boolean fixTxIds;
  private final OfflineEditsVisitor visitor;
  private final InputStreamReader fileReader;
  private ParseState state;
  private Stanza stanza;
  private Stack<Stanza> stanzaStack;
  private FSEditLogOpCodes opCode;
  private StringBuffer cbuf;
  private long nextTxId;
  private final OpInstanceCache opCache = new OpInstanceCache();
  
  static enum ParseState {
    EXPECT_EDITS_TAG,
    EXPECT_VERSION,
    EXPECT_RECORD,
    EXPECT_OPCODE,
    EXPECT_DATA,
    HANDLE_DATA,
    EXPECT_END,
  }
  
  public OfflineEditsXmlLoader(OfflineEditsVisitor visitor,
        File inputFile, OfflineEditsViewer.Flags flags) throws FileNotFoundException {
    this.visitor = visitor;
    this.fileReader =
        new InputStreamReader(new FileInputStream(inputFile), Charsets.UTF_8);
    this.fixTxIds = flags.getFixTxIds();
  }

  /**
   * Loads edits file, uses visitor to process all elements
   */
  @Override
  public void loadEdits() throws IOException {
    try {
      XMLReader xr = XMLReaderFactory.createXMLReader();
      xr.setContentHandler(this);
      xr.setErrorHandler(this);
      xr.setDTDHandler(null);
      xr.parse(new InputSource(fileReader));
      visitor.close(null);
    } catch (SAXParseException e) {
      System.out.println("XML parsing error: " + "\n" +
          "Line:    " + e.getLineNumber() + "\n" +
          "URI:     " + e.getSystemId() + "\n" +
          "Message: " + e.getMessage());        
      visitor.close(e);
      throw new IOException(e.toString());
    } catch (SAXException e) {
      visitor.close(e);
      throw new IOException(e.toString());
    } catch (RuntimeException e) {
      visitor.close(e);
      throw e;
    } finally {
      fileReader.close();
    }
  }
  
  @Override
  public void startDocument() {
    state = ParseState.EXPECT_EDITS_TAG;
    stanza = null;
    stanzaStack = new Stack<Stanza>();
    opCode = null;
    cbuf = new StringBuffer();
    nextTxId = -1;
  }
  
  @Override
  public void endDocument() {
    if (state != ParseState.EXPECT_END) {
      throw new InvalidXmlException("expecting </EDITS>");
    }
  }
  
  @Override
  public void startElement (String uri, String name,
      String qName, Attributes atts) {
    switch (state) {
    case EXPECT_EDITS_TAG:
      if (!name.equals("EDITS")) {
        throw new InvalidXmlException("you must put " +
            "<EDITS> at the top of the XML file! " +
            "Got tag " + name + " instead");
      }
      state = ParseState.EXPECT_VERSION;
      break;
    case EXPECT_VERSION:
      if (!name.equals("EDITS_VERSION")) {
        throw new InvalidXmlException("you must put " +
            "<EDITS_VERSION> at the top of the XML file! " +
            "Got tag " + name + " instead");
      }
      break;
    case EXPECT_RECORD:
      if (!name.equals("RECORD")) {
        throw new InvalidXmlException("expected a <RECORD> tag");
      }
      state = ParseState.EXPECT_OPCODE;
      break;
    case EXPECT_OPCODE:
      if (!name.equals("OPCODE")) {
        throw new InvalidXmlException("expected an <OPCODE> tag");
      }
      break;
    case EXPECT_DATA:
      if (!name.equals("DATA")) {
        throw new InvalidXmlException("expected a <DATA> tag");
      }
      stanza = new Stanza();
      state = ParseState.HANDLE_DATA;
      break;
    case HANDLE_DATA:
      Stanza parent = stanza;
      Stanza child = new Stanza();
      stanzaStack.push(parent);
      stanza = child;
      parent.addChild(name, child);
      break;
    case EXPECT_END:
      throw new InvalidXmlException("not expecting anything after </EDITS>");
    }
  }
  
  @Override
  public void endElement (String uri, String name, String qName) {
    String str = cbuf.toString().trim();
    cbuf = new StringBuffer();
    switch (state) {
    case EXPECT_EDITS_TAG:
      throw new InvalidXmlException("expected <EDITS/>");
    case EXPECT_VERSION:
      if (!name.equals("EDITS_VERSION")) {
        throw new InvalidXmlException("expected </EDITS_VERSION>");
      }
      try {
        int version = Integer.parseInt(str);
        visitor.start(version);
      } catch (IOException e) {
        // Can't throw IOException from a SAX method, sigh.
        throw new RuntimeException(e);
      }
      state = ParseState.EXPECT_RECORD;
      break;
    case EXPECT_RECORD:
      if (name.equals("EDITS")) {
        state = ParseState.EXPECT_END;
      } else if (!name.equals("RECORD")) {
        throw new InvalidXmlException("expected </EDITS> or </RECORD>");
      }
      break;
    case EXPECT_OPCODE:
      if (!name.equals("OPCODE")) {
        throw new InvalidXmlException("expected </OPCODE>");
      }
      opCode = FSEditLogOpCodes.valueOf(str);
      state = ParseState.EXPECT_DATA;
      break;
    case EXPECT_DATA:
      throw new InvalidXmlException("expected <DATA/>");
    case HANDLE_DATA:
      stanza.setValue(str);
      if (stanzaStack.empty()) {
        if (!name.equals("DATA")) {
          throw new InvalidXmlException("expected </DATA>");
        }
        state = ParseState.EXPECT_RECORD;
        FSEditLogOp op = opCache.get(opCode);
        opCode = null;
        try {
          op.decodeXml(stanza);
          stanza = null;
        } finally {
          if (stanza != null) {
            System.err.println("fromXml error decoding opcode " + opCode +
                "\n" + stanza.toString());
            stanza = null;
          }
        }
        if (fixTxIds) {
          if (nextTxId <= 0) {
            nextTxId = op.getTransactionId();
            if (nextTxId <= 0) {
              nextTxId = 1;
            }
          }
          op.setTransactionId(nextTxId);
          nextTxId++;
        }
        try {
          visitor.visitOp(op);
        } catch (IOException e) {
          // Can't throw IOException from a SAX method, sigh.
          throw new RuntimeException(e);
        }
        state = ParseState.EXPECT_RECORD;
      } else {
        stanza = stanzaStack.pop();
      }
      break;
    case EXPECT_END:
      throw new InvalidXmlException("not expecting anything after </EDITS>");
    }
  }
  
  @Override
  public void characters (char ch[], int start, int length) {
    cbuf.append(ch, start, length);
  }
}