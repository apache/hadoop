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

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.TreeMap;
import java.util.ArrayList;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 *
 * @author Milind Bhandarkar
 */
public class BinaryOutputArchive implements OutputArchive {
    
    private DataOutput out;
    
    static BinaryOutputArchive getArchive(OutputStream strm) {
        return new BinaryOutputArchive(new DataOutputStream(strm));
    }
    
    /** Creates a new instance of BinaryOutputArchive */
    public BinaryOutputArchive(DataOutput out) {
        this.out = out;
    }
    
    public void writeByte(byte b, String tag) throws IOException {
        out.writeByte(b);
    }
    
    public void writeBool(boolean b, String tag) throws IOException {
        out.writeBoolean(b);
    }
    
    public void writeInt(int i, String tag) throws IOException {
        Utils.writeInt(out, i);
    }
    
    public void writeLong(long l, String tag) throws IOException {
        Utils.writeLong(out, l);
    }
    
    public void writeFloat(float f, String tag) throws IOException {
        out.writeFloat(f);
    }
    
    public void writeDouble(double d, String tag) throws IOException {
        out.writeDouble(d);
    }
    
    public void writeString(String s, String tag) throws IOException {
        byte[] chars = s.getBytes("UTF-8");
        Utils.writeInt(out, chars.length);
        out.write(chars);
    }
    
    public void writeBuffer(ByteArrayOutputStream buf, String tag)
    throws IOException {
        byte[] barr = buf.toByteArray();
        Utils.writeInt(out, barr.length);
        out.write(barr);
    }
    
    public void writeRecord(Record r, String tag) throws IOException {
        r.serialize(this, tag);
    }
    
    public void startRecord(Record r, String tag) throws IOException {}
    
    public void endRecord(Record r, String tag) throws IOException {}
    
    public void startVector(ArrayList v, String tag) throws IOException {
        Utils.writeInt(out, v.size());
    }
    
    public void endVector(ArrayList v, String tag) throws IOException {}
    
    public void startMap(TreeMap v, String tag) throws IOException {
        Utils.writeInt(out, v.size());
    }
    
    public void endMap(TreeMap v, String tag) throws IOException {}
    
}
