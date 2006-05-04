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

package org.apache.hadoop.record;

import java.io.DataInput;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;


/**
 *
 * @author milindb
 */
public class BinaryInputArchive implements InputArchive {
    
    private DataInput in;
    
    static BinaryInputArchive getArchive(InputStream strm) {
        return new BinaryInputArchive(new DataInputStream(strm));
    }
    
    static private class BinaryIndex implements Index {
        private int nelems;
        BinaryIndex(int nelems) {
            this.nelems = nelems;
        }
        public boolean done() {
            return (nelems <= 0);
        }
        public void incr() {
            nelems--;
        }
    }
    /** Creates a new instance of BinaryInputArchive */
    public BinaryInputArchive(DataInput in) {
        this.in = in;
    }
    
    public byte readByte(String tag) throws IOException {
        return in.readByte();
    }
    
    public boolean readBool(String tag) throws IOException {
        return in.readBoolean();
    }
    
    public int readInt(String tag) throws IOException {
        return Utils.readInt(in);
    }
    
    public long readLong(String tag) throws IOException {
        return Utils.readLong(in);
    }
    
    public float readFloat(String tag) throws IOException {
        return in.readFloat();
    }
    
    public double readDouble(String tag) throws IOException {
        return in.readDouble();
    }
    
    public String readString(String tag) throws IOException {
        int len = Utils.readInt(in);
        byte[] chars = new byte[len];
        in.readFully(chars);
        return new String(chars, "UTF-8");
    }
    
    public ByteArrayOutputStream readBuffer(String tag) throws IOException {
        int len = Utils.readInt(in);
        ByteArrayOutputStream buf = new ByteArrayOutputStream(len);
        byte[] arr = new byte[len];
        in.readFully(arr);
        buf.write(arr, 0, len);
        return buf;
    }
    
    public void readRecord(Record r, String tag) throws IOException {
        r.deserialize(this, tag);
    }
    
    public void startRecord(String tag) throws IOException {}
    
    public void endRecord(String tag) throws IOException {}
    
    public Index startVector(String tag) throws IOException {
        return new BinaryIndex(Utils.readInt(in));
    }
    
    public void endVector(String tag) throws IOException {}
    
    public Index startMap(String tag) throws IOException {
        return new BinaryIndex(Utils.readInt(in));
    }
    
    public void endMap(String tag) throws IOException {}
    
}
