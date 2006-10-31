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

package org.apache.hadoop.record;

import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.PushbackReader;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.io.Text;

/**
 *
 * @author Milind Bhandarkar
 */
class CsvInputArchive implements InputArchive {
    
    private PushbackReader stream;
    
    private class CsvIndex implements Index {
        public boolean done() {
            char c = '\0';
            try {
                c = (char) stream.read();
                stream.unread(c);
            } catch (IOException ex) {
            }
            return (c == '}') ? true : false;
        }
        public void incr() {}
    }
    
    private void throwExceptionOnError(String tag) throws IOException {
        throw new IOException("Error deserializing "+tag);
    }
    
    private String readField(String tag) throws IOException {
        try {
            StringBuffer buf = new StringBuffer();
            while (true) {
                char c = (char) stream.read();
                switch (c) {
                    case ',':
                        return buf.toString();
                    case '}':
                    case '\n':
                    case '\r':
                        stream.unread(c);
                        return buf.toString();
                    default:
                        buf.append(c);
                }
            }
        } catch (IOException ex) {
            throw new IOException("Error reading "+tag);
        }
    }
    
    static CsvInputArchive getArchive(InputStream strm)
    throws UnsupportedEncodingException {
        return new CsvInputArchive(strm);
    }
    
    /** Creates a new instance of CsvInputArchive */
    public CsvInputArchive(InputStream in)
    throws UnsupportedEncodingException {
        stream = new PushbackReader(new InputStreamReader(in, "UTF-8"));
    }
    
    public byte readByte(String tag) throws IOException {
        return (byte) readLong(tag);
    }
    
    public boolean readBool(String tag) throws IOException {
        String sval = readField(tag);
        return "T".equals(sval) ? true : false;
    }
    
    public int readInt(String tag) throws IOException {
        return (int) readLong(tag);
    }
    
    public long readLong(String tag) throws IOException {
        String sval = readField(tag);
        try {
            long lval = Long.parseLong(sval);
            return lval;
        } catch (NumberFormatException ex) {
            throw new IOException("Error deserializing "+tag);
        }
    }
    
    public float readFloat(String tag) throws IOException {
        return (float) readDouble(tag);
    }
    
    public double readDouble(String tag) throws IOException {
        String sval = readField(tag);
        try {
            double dval = Double.parseDouble(sval);
            return dval;
        } catch (NumberFormatException ex) {
            throw new IOException("Error deserializing "+tag);
        }
    }
    
    public Text readString(String tag) throws IOException {
        String sval = readField(tag);
        return Utils.fromCSVString(sval);
        
    }
    
    public ByteArrayOutputStream readBuffer(String tag) throws IOException {
        String sval = readField(tag);
        return Utils.fromCSVBuffer(sval);
    }
    
    public void readRecord(Record r, String tag) throws IOException {
        r.deserialize(this, tag);
    }
    
    public void startRecord(String tag) throws IOException {
        if (tag != null && !"".equals(tag)) {
            char c1 = (char) stream.read();
            char c2 = (char) stream.read();
            if (c1 != 's' || c2 != '{') {
                throw new IOException("Error deserializing "+tag);
            }
        }
    }
    
    public void endRecord(String tag) throws IOException {
        char c = (char) stream.read();
        if (tag == null || "".equals(tag)) {
            if (c != '\n' && c != '\r') {
                throw new IOException("Error deserializing record.");
            } else {
                return;
            }
        }
        
        if (c != '}') {
            throw new IOException("Error deserializing "+tag);
        }
        c = (char) stream.read();
        if (c != ',') {
            stream.unread(c);
        }
        
        return;
    }
    
    public Index startVector(String tag) throws IOException {
        char c1 = (char) stream.read();
        char c2 = (char) stream.read();
        if (c1 != 'v' || c2 != '{') {
            throw new IOException("Error deserializing "+tag);
        }
        return new CsvIndex();
    }
    
    public void endVector(String tag) throws IOException {
        char c = (char) stream.read();
        if (c != '}') {
            throw new IOException("Error deserializing "+tag);
        }
        c = (char) stream.read();
        if (c != ',') {
            stream.unread(c);
        }
        return;
    }
    
    public Index startMap(String tag) throws IOException {
        char c1 = (char) stream.read();
        char c2 = (char) stream.read();
        if (c1 != 'm' || c2 != '{') {
            throw new IOException("Error deserializing "+tag);
        }
        return new CsvIndex();
    }
    
    public void endMap(String tag) throws IOException {
        char c = (char) stream.read();
        if (c != '}') {
            throw new IOException("Error deserializing "+tag);
        }
        c = (char) stream.read();
        if (c != ',') {
            stream.unread(c);
        }
        return;
    }
}
