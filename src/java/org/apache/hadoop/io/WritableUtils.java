/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io;

import java.io.*;
import org.apache.hadoop.mapred.JobConf;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class WritableUtils  {

  public static byte[] readCompressedByteArray(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == -1) return null;
    byte[] buffer = new byte[length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buffer, 0, buffer.length));
    byte[] outbuf = new byte[length];
    ByteArrayOutputStream bos =  new ByteArrayOutputStream();
     int len;
     while((len=gzi.read(outbuf,0,outbuf.length)) != -1){
       bos.write(outbuf,0,len);
     }
     byte[] decompressed =  bos.toByteArray();
     bos.close();
     gzi.close();
     return decompressed;
  }

  public static void skipCompressedByteArray(DataInput in) throws IOException {
    int length = in.readInt();
    if (length != -1) in.skipBytes(length);
  }

  public static int  writeCompressedByteArray(DataOutput out, byte[] bytes) throws IOException {
    if (bytes != null) {
      ByteArrayOutputStream bos =  new ByteArrayOutputStream();
      GZIPOutputStream gzout = new GZIPOutputStream(bos);
      gzout.write(bytes,0,bytes.length);
      gzout.close();
      byte[] buffer = bos.toByteArray();
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer,0,len);
    /* debug only! Once we have confidence, can lose this. */
      return ((bytes.length != 0) ? (100*buffer.length)/bytes.length : 0);
    } else {
      out.writeInt(-1);
      return -1;
    }
  }


  /* Ugly utility, maybe someone else can do this better  */
  public static String readCompressedString(DataInput in) throws IOException {
    byte[] bytes = readCompressedByteArray(in);
    if (bytes == null) return null;
    return new String(bytes, "UTF-8");
  }


  public static int  writeCompressedString(DataOutput out, String s) throws IOException {
    return writeCompressedByteArray(out, (s != null) ? s.getBytes("UTF-8") : null);
  }

  /*
   *
   * Write a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   * 
   */
  public static void writeString(DataOutput out, String s) throws IOException {
    if (s != null) {
      byte[] buffer = s.getBytes("UTF-8");
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer,0,len);
    } else {
      out.writeInt(-1);
    }
  }

  /*
   * Read a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   *
   */
  public static String readString(DataInput in) throws IOException{
    int length = in.readInt();
    if (length == -1) return null;
    byte[] buffer = new byte[length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    return new String(buffer,"UTF-8");  
  }


  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection.
   *
   */
  public static void writeStringArray(DataOutput out, String[] s) throws IOException{
    out.writeInt(s.length);
    for(int i = 0; i < s.length; i++) {
      writeString(out, s[i]);
    }
  }

  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array of
   * compressed Strings. Handles also null arrays and null values.
   * Could be generalised using introspection.
   *
   */
  public static void writeCompressedStringArray(DataOutput out, String[] s) throws IOException{
    if (s == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(s.length);
    for(int i = 0; i < s.length; i++) {
      writeCompressedString(out, s[i]);
    }
  }

  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection. Actually this bit couldn't...
   *
   */
  public static String[] readStringArray(DataInput in) throws IOException {
    int len = in.readInt();
    if (len == -1) return null;
    String[] s = new String[len];
    for(int i = 0; i < len; i++) {
      s[i] = readString(in);
    }
    return s;
  }


  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection. Handles null arrays and null values.
   *
   */
  public static  String[] readCompressedStringArray(DataInput in) throws IOException {
    int len = in.readInt();
    if (len == -1) return null;
    String[] s = new String[len];
    for(int i = 0; i < len; i++) {
      s[i] = readCompressedString(in);
    }
    return s;
  }


  /*
   *
   * Test Utility Method Display Byte Array. 
   *
   */
  public static void displayByteArray(byte[] record){
    int i;
    for(i=0;i < record.length -1 ; i++){
      if (i % 16 == 0) { System.out.println(); }
      System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
      System.out.print(Integer.toHexString(record[i] & 0x0F));
      System.out.print(",");
    }
    System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
    System.out.print(Integer.toHexString(record[i] & 0x0F));
    System.out.println();
  }

  /**
   * A pair of input/output buffers that we use to clone writables.
   */
  private static class CopyInCopyOutBuffer {
    DataOutputBuffer outBuffer = new DataOutputBuffer();
    DataInputBuffer inBuffer = new DataInputBuffer();
    /**
     * Move the data from the output buffer to the input buffer.
     */
    void moveData() {
      inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
    }
  }
  
  /**
   * Allocate a buffer for each thread that tries to clone objects.
   */
  private static ThreadLocal cloneBuffers = new ThreadLocal() {
    protected synchronized Object initialValue() {
      return new CopyInCopyOutBuffer();
    }
  };
  
  /**
   * Make a copy of a writable object using serialization to a buffer.
   * @param orig The object to copy
   * @return The copied object
   */
  public static Writable clone(Writable orig, JobConf conf) {
    try {
      Writable newInst = (Writable)conf.newInstance(orig.getClass());
      CopyInCopyOutBuffer buffer = (CopyInCopyOutBuffer)cloneBuffers.get();
      buffer.outBuffer.reset();
      orig.write(buffer.outBuffer);
      buffer.moveData();
      newInst.readFields(buffer.inBuffer);
      return newInst;
    } catch (IOException e) {
      throw new RuntimeException("Error writing/reading clone buffer", e);
    }
  }
  
}
