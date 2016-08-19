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
package org.apache.hadoop.oncrpc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Utility class for building XDR messages based on RFC 4506.
 *
 * Key points of the format:
 *
 * <ul>
 * <li>Primitives are stored in big-endian order (i.e., the default byte order
 * of ByteBuffer).</li>
 * <li>Booleans are stored as an integer.</li>
 * <li>Each field in the message is always aligned by 4.</li>
 * </ul>
 *
 */
public final class XDR {
  private static final int DEFAULT_INITIAL_CAPACITY = 256;
  private static final int SIZEOF_INT = 4;
  private static final int SIZEOF_LONG = 8;
  private static final byte[] PADDING_BYTES = new byte[] { 0, 0, 0, 0 };

  private ByteBuffer buf;

  public enum State {
    READING, WRITING,
  }

  private final State state;

  /**
   * Construct a new XDR message buffer.
   *
   * @param initialCapacity
   *          the initial capacity of the buffer.
   */
  public XDR(int initialCapacity) {
    this(ByteBuffer.allocate(initialCapacity), State.WRITING);
  }

  public XDR() {
    this(DEFAULT_INITIAL_CAPACITY);
  }

  public XDR(ByteBuffer buf, State state) {
    this.buf = buf;
    this.state = state;
  }

  /**
   * Wraps a byte array as a read-only XDR message. There's no copy involved,
   * thus it is the client's responsibility to ensure that the byte array
   * remains unmodified when using the XDR object.
   * 
   * @param src
   *          the byte array to be wrapped.
   */
  public XDR(byte[] src) {
    this(ByteBuffer.wrap(src).asReadOnlyBuffer(), State.READING);
  }

  public XDR asReadOnlyWrap() {
    ByteBuffer b = buf.asReadOnlyBuffer();
    if (state == State.WRITING) {
      b.flip();
    }

    XDR n = new XDR(b, State.READING);
    return n;
  }

  public ByteBuffer buffer() {
    return buf.duplicate();
  }

  public int size() {
    // TODO: This overloading intends to be compatible with the semantics of
    // the previous version of the class. This function should be separated into
    // two with clear semantics.
    return state == State.READING ? buf.limit() : buf.position();
  }

  public int readInt() {
    Preconditions.checkState(state == State.READING);
    return buf.getInt();
  }

  public void writeInt(int v) {
    ensureFreeSpace(SIZEOF_INT);
    buf.putInt(v);
  }

  public boolean readBoolean() {
    Preconditions.checkState(state == State.READING);
    return buf.getInt() != 0;
  }

  public void writeBoolean(boolean v) {
    ensureFreeSpace(SIZEOF_INT);
    buf.putInt(v ? 1 : 0);
  }

  public long readHyper() {
    Preconditions.checkState(state == State.READING);
    return buf.getLong();
  }

  public void writeLongAsHyper(long v) {
    ensureFreeSpace(SIZEOF_LONG);
    buf.putLong(v);
  }

  public byte[] readFixedOpaque(int size) {
    Preconditions.checkState(state == State.READING);
    byte[] r = new byte[size];
    buf.get(r);
    alignPosition();
    return r;
  }

  public void writeFixedOpaque(byte[] src, int length) {
    ensureFreeSpace(alignUp(length));
    buf.put(src, 0, length);
    writePadding();
  }

  public void writeFixedOpaque(byte[] src) {
    writeFixedOpaque(src, src.length);
  }

  public byte[] readVariableOpaque() {
    Preconditions.checkState(state == State.READING);
    int size = readInt();
    return readFixedOpaque(size);
  }

  public void writeVariableOpaque(byte[] src) {
    ensureFreeSpace(SIZEOF_INT + alignUp(src.length));
    buf.putInt(src.length);
    writeFixedOpaque(src);
  }

  public String readString() {
    return new String(readVariableOpaque(), StandardCharsets.UTF_8);
  }

  public void writeString(String s) {
    writeVariableOpaque(s.getBytes(StandardCharsets.UTF_8));
  }

  private void writePadding() {
    Preconditions.checkState(state == State.WRITING);
    int p = pad(buf.position());
    ensureFreeSpace(p);
    buf.put(PADDING_BYTES, 0, p);
  }

  private int alignUp(int length) {
    return length + pad(length);
  }

  private int pad(int length) {
    switch (length % 4) {
    case 1:
      return 3;
    case 2:
      return 2;
    case 3:
      return 1;
    default:
      return 0;
    }
  }

  private void alignPosition() {
    buf.position(alignUp(buf.position()));
  }

  private void ensureFreeSpace(int size) {
    Preconditions.checkState(state == State.WRITING);
    if (buf.remaining() < size) {
      int newCapacity = buf.capacity() * 2;
      int newRemaining = buf.capacity() + buf.remaining();

      while (newRemaining < size) {
        newRemaining += newCapacity;
        newCapacity *= 2;
      }

      ByteBuffer newbuf = ByteBuffer.allocate(newCapacity);
      buf.flip();
      newbuf.put(buf);
      buf = newbuf;
    }
  }

  /**
   * check if the rest of data has more than len bytes.
   * @param xdr XDR message
   * @param len minimum remaining length
   * @return specify remaining length is enough or not
   */
  public static boolean verifyLength(XDR xdr, int len) {
    return xdr.buf.remaining() >= len;
  }

  static byte[] recordMark(int size, boolean last) {
    byte[] b = new byte[SIZEOF_INT];
    ByteBuffer buf = ByteBuffer.wrap(b);
    buf.putInt(!last ? size : size | 0x80000000);
    return b;
  }

  /**
   * Write an XDR message to a TCP ChannelBuffer.
   * @param request XDR request
   * @param last specifies last request or not
   * @return TCP buffer
   */
  public static ChannelBuffer writeMessageTcp(XDR request, boolean last) {
    Preconditions.checkState(request.state == XDR.State.WRITING);
    ByteBuffer b = request.buf.duplicate();
    b.flip();
    byte[] fragmentHeader = XDR.recordMark(b.limit(), last);
    ByteBuffer headerBuf = ByteBuffer.wrap(fragmentHeader);

    // TODO: Investigate whether making a copy of the buffer is necessary.
    return ChannelBuffers.copiedBuffer(headerBuf, b);
  }

  /**
   * Write an XDR message to a UDP ChannelBuffer.
   * @param response XDR response
   * @return UDP buffer
   */
  public static ChannelBuffer writeMessageUdp(XDR response) {
    Preconditions.checkState(response.state == XDR.State.READING);
    // TODO: Investigate whether making a copy of the buffer is necessary.
    return ChannelBuffers.copiedBuffer(response.buf);
  }

  public static int fragmentSize(byte[] mark) {
    ByteBuffer b = ByteBuffer.wrap(mark);
    int n = b.getInt();
    return n & 0x7fffffff;
  }

  public static boolean isLastFragment(byte[] mark) {
    ByteBuffer b = ByteBuffer.wrap(mark);
    int n = b.getInt();
    return (n & 0x80000000) != 0;
  }

  @VisibleForTesting
  public byte[] getBytes() {
    ByteBuffer d = asReadOnlyWrap().buffer();
    byte[] b = new byte[d.remaining()];
    d.get(b);

    return b;
  }
}
