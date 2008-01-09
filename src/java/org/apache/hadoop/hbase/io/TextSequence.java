/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * As CharSequence is to String, so is TextSequence to {@link Text}
 * (except a TextSequence is a Text whereas a String is a CharSequence). Use
 * when you want to conserve on object creation.
 * 
 * <p>Use with care. If danger that the passed in {@link Text} instance can
 * change during the life of this TextSequence, concretize this TextSequence
 * by calling {@link #toText()}.
 * 
 * <p>Equals considers a Text equal if the TextSequence brackets the same bytes.
 * 
 * <p>TextSequence will not always work as a Text.  For instance, the following
 * fails <code>Text c = new Text(new TextSequence(new Text("some string")));
 * </code> because the Text constructor accesses private Text data members
 * making the new instance from the passed 'Text'.
 * 
 * <p>TODO: Should this be an Interface as CharSequence is?
 */
public class TextSequence extends Text {
  private Text delegatee;
  private int start = 0;
  private int end = -1;
  
  public TextSequence() {
    super();
    this.delegatee = new Text();
  }
  
  public TextSequence(final Text d) {
    this(d, 0);
  }
  
  public TextSequence(final Text d, final int s) {
    this(d, s, d.getLength());
  }
  
  public TextSequence(final Text d, final int s, final int e) {
    this.delegatee = d;
    if (s < 0 || s >= d.getLength()) {
      throw new IllegalArgumentException("Nonsensical start position " + s);
    }
    this.start = s;
    if (e == -1) {
      this.end = this.delegatee.getLength();
    } else if (e <= 0 || e > d.getLength()) {
      throw new IllegalArgumentException("Nonsensical start position " + s);
    } else {
      this.end = e;
    }
  }

  public int charAt(int position) {
    if (position + this.start > this.end ||
        position + this.start < this.start) {
      return -1;
    }
    return this.delegatee.charAt(start + position);
  }

  public int compareTo(Object o) {
    if (o instanceof TextSequence) {
      TextSequence that = (TextSequence)o;
      if (this == that) {
        return 0;
      }
      return WritableComparator.compareBytes(this.delegatee.getBytes(),
        this.start, this.getLength(),
        that.delegatee.getBytes(), that.start, that.getLength());
    }
    // Presume type is Text as super method does.
    Text that = (Text)o;
    return WritableComparator.compareBytes(this.delegatee.getBytes(),
      this.start, this.getLength(), that.getBytes(), 0, that.getLength());
  }

  public boolean equals(Object o) {
    return compareTo(o) == 0;
  }

  public int find(String what, int s) {
    return this.delegatee.find(what, this.start + s) - this.start;
  }

  public int find(String what) {
    return find(what, 0);
  }

  public byte[] getBytes() {
    byte [] b  = new byte [getLength()];
    System.arraycopy(this.delegatee.getBytes(), this.start, b, 0, getLength());
    return b;
  }
  
  /**
   * @return A new Text instance made from the bytes this TextSequence covers.
   */
  public Text toText() {
    return new Text(getBytes());
  }

  public int getLength() {
    return this.end == -1? this.delegatee.getLength(): this.end - this.start;
  }

  public int hashCode() {
    int hash = 1;
    byte [] b = this.delegatee.getBytes();
    for (int i = this.start, length = getLength(); i < length; i++)
      hash = (31 * hash) + b[i];
    return hash;
  }

  public void set(byte[] utf8, int start, int len) {
    this.delegatee.set(utf8, start, len);
  }

  public void set(byte[] utf8) {
    this.delegatee.set(utf8);
  }

  public void set(String string) {
    this.delegatee.set(string);
  }

  public void set(Text other) {
    this.delegatee.set(other);
    this.start = 0;
    this.end = other.getLength();
  }

  public String toString() {
    return this.delegatee.toString().substring(this.start, this.end);
  }


  public void readFields(DataInput in) throws IOException {
    this.start = in.readInt();
    this.end = in.readInt();
    this.delegatee.readFields(in);
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.start);
    out.writeInt(this.end);
    this.delegatee.write(out);
  }
}