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
package org.apache.hadoop.mapred.nativetask.util;

import com.google.common.base.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class ReadWriteBuffer {
  private byte[] _buff;
  private int _writePoint;
  private int _readPoint;
  final static int CACHE_LINE_SIZE = 16;

  public ReadWriteBuffer(int length) {
    if (length > 0) {
      _buff = new byte[length];
    }
  }

  public ReadWriteBuffer() {
    _buff = new byte[CACHE_LINE_SIZE];
  }

  public ReadWriteBuffer(byte[] bytes) {
    _buff = bytes;
    _writePoint = 0;
    _readPoint = 0;
  }

  public void reset(byte[] newBuff) {
    _buff = newBuff;
    _writePoint = 0;
    _readPoint = 0;
  }

  public void setReadPoint(int pos) {
    _readPoint = pos;
  }

  public void setWritePoint(int pos) {
    _writePoint = pos;
  }

  public byte[] getBuff() {
    return _buff;
  }

  public int getWritePoint() {
    return _writePoint;
  }

  public int getReadPoint() {
    return _readPoint;
  }

  public void writeInt(int v) {
    checkWriteSpaceAndResizeIfNecessary(4);

    _buff[_writePoint + 0] = (byte) ((v >>> 0) & 0xFF);
    _buff[_writePoint + 1] = (byte) ((v >>> 8) & 0xFF);
    _buff[_writePoint + 2] = (byte) ((v >>> 16) & 0xFF);
    _buff[_writePoint + 3] = (byte) ((v >>> 24) & 0xFF);

    _writePoint += 4;
  }

  public void writeLong(long v) {
    checkWriteSpaceAndResizeIfNecessary(8);

    _buff[_writePoint + 0] = (byte) (v >>> 0);
    _buff[_writePoint + 1] = (byte) (v >>> 8);
    _buff[_writePoint + 2] = (byte) (v >>> 16);
    _buff[_writePoint + 3] = (byte) (v >>> 24);
    _buff[_writePoint + 4] = (byte) (v >>> 32);
    _buff[_writePoint + 5] = (byte) (v >>> 40);
    _buff[_writePoint + 6] = (byte) (v >>> 48);
    _buff[_writePoint + 7] = (byte) (v >>> 56);

    _writePoint += 8;
  }

  public void writeBytes(byte b[], int off, int len) {
    writeInt(len);
    checkWriteSpaceAndResizeIfNecessary(len);
    System.arraycopy(b, off, _buff, _writePoint, len);
    _writePoint += len;
  }

  public int readInt() {
    final int ch4 = 0xff & (_buff[_readPoint + 0]);
    final int ch3 = 0xff & (_buff[_readPoint + 1]);
    final int ch2 = 0xff & (_buff[_readPoint + 2]);
    final int ch1 = 0xff & (_buff[_readPoint + 3]);
    _readPoint += 4;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  public long readLong() {
    final long result =
      ((_buff[_readPoint + 0] & 255) << 0) +
      ((_buff[_readPoint + 1] & 255) << 8) +
      ((_buff[_readPoint + 2] & 255) << 16) +
      ((long) (_buff[_readPoint + 3] & 255) << 24) +
      ((long) (_buff[_readPoint + 4] & 255) << 32) +
      ((long) (_buff[_readPoint + 5] & 255) << 40) +
      ((long) (_buff[_readPoint + 6] & 255) << 48) +
      (((long) _buff[_readPoint + 7] << 56));

    _readPoint += 8;
    return result;
  }

  public byte[] readBytes() {
    final int length = readInt();
    final byte[] result = new byte[length];
    System.arraycopy(_buff, _readPoint, result, 0, length);
    _readPoint += length;
    return result;
  }

  public void writeString(String str) {
    final byte[] bytes = str.getBytes(Charsets.UTF_8);
    writeBytes(bytes, 0, bytes.length);
  }

  public String readString() {
    final byte[] bytes = readBytes();
    return new String(bytes, Charsets.UTF_8);
  }

  private void checkWriteSpaceAndResizeIfNecessary(int toBeWritten) {

    if (_buff.length - _writePoint >= toBeWritten) {
      return;
    }
    final int newLength = (toBeWritten + _writePoint > CACHE_LINE_SIZE) ?
      (toBeWritten + _writePoint) : CACHE_LINE_SIZE;
    final byte[] newBuff = new byte[newLength];
    System.arraycopy(_buff, 0, newBuff, 0, _writePoint);
    _buff = newBuff;
  }

};
