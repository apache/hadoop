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

package org.apache.hadoop.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

import org.apache.hadoop.io.Text;

/**
 * General utils for byte array containing UTF-8 encoded strings
 * @author hairong 
 */

public class UTF8ByteArrayUtils {
    /**
     * Find the first occured tab in a UTF-8 encoded string
     * @param utf: a byte array containing a UTF-8 encoded string
     * @return position that first tab occures otherwise -1
     */
    public static int findTab(byte [] utf) {
        for(int i=0; i<utf.length; i++) {
            if(utf[i]==(byte)'\t') {
                return i;
            }
          }
          return -1;      
    }
    
    /**
     * split a UTF-8 byte array into key and value 
     * assuming that the delimilator is at splitpos. 
     * @param ut: utf-8 encoded string
     * @param key: contains key upon the method is returned
     * @param val: contains value upon the method is returned
     * @param splitPos: the split pos
     * @throws IOException: when 
     */
    public static void splitKeyVal(byte[] utf, Text key, Text val, int splitPos) 
    throws IOException {
        if(splitPos<0 || splitPos >= utf.length)
            throw new IllegalArgumentException(
                    "splitPos must be in the range [0, "+splitPos+"]: " +splitPos);
        byte [] keyBytes = new byte[splitPos];
        System.arraycopy(utf, 0, keyBytes, 0, splitPos);
        int valLen = utf.length-splitPos-1;
        byte [] valBytes = new byte[valLen];
        System.arraycopy(utf,splitPos+1, valBytes, 0, valLen );
        key.set(keyBytes);
        val.set(valBytes);
    }
    
    /**
     * Read a utf8 encoded line from a data input stream. 
     * @param in data input stream
     * @return a byte array containing the line 
     * @throws IOException
     */
    public static byte[] readLine(InputStream in) throws IOException {
      byte [] buf = new byte[128];
      byte [] lineBuffer = buf;
      int room = 128;
      int offset = 0;
      boolean isEOF = false;
      while (true) {
        int b = in.read();
        if (b == -1) {
          isEOF = true;
          break;
        }

        char c = (char)b;
        if (c == '\n')
          break;

        if (c == '\r') {
          in.mark(1);
          int c2 = in.read();
          if(c2 == -1) {
              isEOF = true;
              break;
          }
          if (c2 != '\n') {
            // push it back
            in.reset();
          }
          break;
        }
        
        if (--room < 0) {
            buf = new byte[offset + 128];
            room = buf.length - offset - 1;
            System.arraycopy(lineBuffer, 0, buf, 0, offset);
            lineBuffer = buf;
        }
        buf[offset++] = (byte) c;
      }

      if(isEOF && offset==0) {
          return null;
      } else {
          lineBuffer = new byte[offset];
          System.arraycopy(buf, 0, lineBuffer, 0, offset);
          return lineBuffer;
      }
    }
}
