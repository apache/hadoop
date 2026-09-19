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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.junit.jupiter.api.Test;

public class TestFSEditLogOp {

  @Test
  public void testFromXmlWithoutBlockResetsTruncateBlock()
      throws InvalidXmlException {
    FSEditLogOp.TruncateOp op = new FSEditLogOp.TruncateOp();
    op.setPath("/src")
        .setClientName("client")
        .setClientMachine("machine")
        .setNewLength(123L)
        .setTimestamp(10L)
        .setTruncateBlock(new Block(1L, 2L, 3L));

    Stanza root = new Stanza();
    root.addChild("SRC", stanzaValue("/src"));
    root.addChild("CLIENTNAME", stanzaValue("client"));
    root.addChild("CLIENTMACHINE", stanzaValue("machine"));
    root.addChild("NEWLENGTH", stanzaValue("123"));
    root.addChild("TIMESTAMP", stanzaValue("10"));

    op.fromXml(root);

    assertEquals("/src", op.src);
    assertEquals("client", op.clientName);
    assertEquals("machine", op.clientMachine);
    assertEquals(123L, op.newLength);
    assertEquals(10L, op.timestamp);
    assertNull(op.truncateBlock);
  }

  @Test
  public void testFromXmlWithBlockSetsTruncateBlock()
      throws InvalidXmlException {
    FSEditLogOp.TruncateOp op = new FSEditLogOp.TruncateOp();

    Stanza root = new Stanza();
    root.addChild("SRC", stanzaValue("/src2"));
    root.addChild("CLIENTNAME", stanzaValue("client2"));
    root.addChild("CLIENTMACHINE", stanzaValue("machine2"));
    root.addChild("NEWLENGTH", stanzaValue("200"));
    root.addChild("TIMESTAMP", stanzaValue("20"));
    root.addChild("BLOCK", blockStanza(7L, 100L, 9L));

    op.fromXml(root);

    assertEquals("/src2", op.src);
    assertEquals("client2", op.clientName);
    assertEquals("machine2", op.clientMachine);
    assertEquals(200L, op.newLength);
    assertEquals(20L, op.timestamp);
    assertNotNull(op.truncateBlock);
    assertEquals(7L, op.truncateBlock.getBlockId());
    assertEquals(100L, op.truncateBlock.getNumBytes());
    assertEquals(9L, op.truncateBlock.getGenerationStamp());
  }

  /**
   * When stanza has multiple BLOCK children, fromXml uses the first one
   * (st.getChildren("BLOCK").get(0)).
   */
  @Test
  public void testFromXmlWithMultipleBlocksUsesFirst()
      throws InvalidXmlException {
    FSEditLogOp.TruncateOp op = new FSEditLogOp.TruncateOp();

    Stanza root = new Stanza();
    root.addChild("SRC", stanzaValue("/path"));
    root.addChild("CLIENTNAME", stanzaValue("c"));
    root.addChild("CLIENTMACHINE", stanzaValue("m"));
    root.addChild("NEWLENGTH", stanzaValue("0"));
    root.addChild("TIMESTAMP", stanzaValue("0"));
    root.addChild("BLOCK", blockStanza(1L, 10L, 100L));
    root.addChild("BLOCK", blockStanza(2L, 20L, 200L));

    op.fromXml(root);

    assertNotNull(op.truncateBlock);
    assertEquals(1L, op.truncateBlock.getBlockId());
    assertEquals(10L, op.truncateBlock.getNumBytes());
    assertEquals(100L, op.truncateBlock.getGenerationStamp());
  }

  private static Stanza stanzaValue(String value) {
    Stanza s = new Stanza();
    s.setValue(value);
    return s;
  }

  private static Stanza blockStanza(long blockId, long numBytes,
      long genstamp) {
    Stanza block = new Stanza();
    block.addChild("BLOCK_ID", stanzaValue(Long.toString(blockId)));
    block.addChild("NUM_BYTES", stanzaValue(Long.toString(numBytes)));
    block.addChild("GENSTAMP", stanzaValue(Long.toString(genstamp)));
    return block;
  }
}
