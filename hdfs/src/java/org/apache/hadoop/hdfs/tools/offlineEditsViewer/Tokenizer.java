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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.hdfs.tools.offlineEditsViewer.EditsElement;

/**
 * Tokenizer that hides the details of different input formats
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
interface Tokenizer {

  /**
   * Abstract class Token, derive Tokens of needed types from
   * this class
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  abstract public class Token {
    EditsElement e;

    /**
     * Constructor
     */
    public Token(EditsElement e) { this.e = e; }

    /**
     * EditsElement accessor
     *
     * @return EditsElement of this Token
     */
    public EditsElement getEditsElement() { return e; }

    /**
     * Creates token from a string
     *
     * @param string a string to set the value of token
     */
    abstract public void fromString(String s) throws IOException;

    /**
     * Creates token from binary stream
     *
     * @param in input stream to read token value from
     */
    abstract public void fromBinary(DataInputStream in) throws IOException;

    /**
     * Converts token to string
     */
    abstract public String toString();

    /**
     * Writes token value in binary format to out
     *
     * @param out output stream to write value to
     */
    abstract public void toBinary(DataOutputStream out) throws IOException;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class ByteToken extends Token {
    public byte value;
    public ByteToken(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {
      value = Byte.valueOf(s);
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      value = in.readByte();
    }

    @Override
    public String toString() {
      return Byte.toString(value);
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      out.writeByte(value);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class ShortToken extends Token {
    public short value;
    public ShortToken(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {
      value = Short.parseShort(s);
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      value = in.readShort();
    }

    @Override
    public String toString() {
      return Short.toString(value);
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      out.writeShort(value);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class IntToken extends Token {
    public int value;
    public IntToken(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {
      value = Integer.parseInt(s);
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      value = in.readInt();
    }

    @Override
    public String toString() {
      return Integer.toString(value);
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      out.writeInt(value);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class VIntToken extends Token {
    public int value;
    public VIntToken(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {
      value = Integer.parseInt(s);
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      value = WritableUtils.readVInt(in);
    }

    @Override
    public String toString() {
      return Integer.toString(value);
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      WritableUtils.writeVInt(out, value);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class LongToken extends Token {
    public long value;
    public LongToken(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {
      value = Long.parseLong(s);
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      value = in.readLong();
    }

    @Override
    public String toString() {
      return Long.toString(value);
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      out.writeLong(value);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class VLongToken extends Token {
    public long value;
    public VLongToken(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {
      value = Long.parseLong(s);
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      value = WritableUtils.readVLong(in);
    }

    @Override
    public String toString() {
      return Long.toString(value);
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      WritableUtils.writeVLong(out, value);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class StringUTF8Token extends Token {
    public String value;
    public StringUTF8Token(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {
      value = s;
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      value = DeprecatedUTF8.readString(in);
    }

    @Override
    public String toString() {
      return value;
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      DeprecatedUTF8.writeString(out, value);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class StringTextToken extends Token {
    public String value;
    public StringTextToken(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {
      value = s;
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      value = Text.readString(in);
    }

    @Override
    public String toString() {
      return value;
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      Text.writeString(out, value);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class BlobToken extends Token {
    public byte[] value = null;
    public BlobToken(EditsElement e, int length) {
      super(e);
      value = (length == -1) ? null : new byte[length];
    }

    @Override
    public void fromString(String s) throws IOException {
      value = Base64.decodeBase64(s);
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      in.readFully(value);
    }

    @Override
    public String toString() {
      return Base64.encodeBase64URLSafeString(value);
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      out.write(value);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class BytesWritableToken extends Token {
    public BytesWritable value = new BytesWritable();
    public BytesWritableToken(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {
      value = new BytesWritable(Base64.decodeBase64(s));
    }

    @Override
    public void fromBinary(DataInputStream in) throws IOException {
      value.readFields(in);
    }

    @Override
    public String toString() {
      return Base64.encodeBase64URLSafeString(value.getBytes());
    }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {
      value.write(out);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class EmptyToken extends Token {
    public EmptyToken(EditsElement e) { super(e); }

    @Override
    public void fromString(String s) throws IOException {}

    @Override
    public void fromBinary(DataInputStream in) throws IOException {}

    @Override
    public String toString() { return ""; }

    @Override
    public void toBinary(DataOutputStream out) throws IOException {}
  }

  /**
   * Read a Token, note that there is no write function
   * because writing is handled by Visitor and individual
   * toString/toBinary functions for given Token implementations.
   *
   * Note that it works on the token it gets as a parameter
   * and returns the same token, this is done so that it can be
   * called in pipe-like pattern token = f1(f2(f3())), where f3
   * creates an instance of Token.
   *
   * @param t token to read
   * @return token that was just read
   */
  public Token read(Token t) throws IOException;
}
