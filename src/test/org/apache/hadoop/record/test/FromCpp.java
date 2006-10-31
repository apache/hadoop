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

package org.apache.hadoop.record.test;

import org.apache.hadoop.record.RecordReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import junit.framework.*;
import org.apache.hadoop.io.Text;

/**
 *
 * @author Milind Bhandarkar
 */
public class FromCpp extends TestCase {
    
    public FromCpp(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
    }

    protected void tearDown() throws Exception {
    }
    
    public void testBinary() {
        File tmpfile;
        try {
            tmpfile = new File("/temp/hadooptmp.dat");
            RecRecord1 r1 = new RecRecord1();
            r1.setBoolVal(true);
            r1.setByteVal((byte)0x66);
            r1.setFloatVal(3.145F);
            r1.setDoubleVal(1.5234);
            r1.setIntVal(4567);
            r1.setLongVal(0x5a5a5a5a5a5aL);
            r1.setStringVal(new Text("random text"));
            r1.setBufferVal(new ByteArrayOutputStream(20));
            r1.setVectorVal(new ArrayList());
            r1.setMapVal(new TreeMap());
            FileInputStream istream = new FileInputStream(tmpfile);
            RecordReader in = new RecordReader(istream, "binary");
            RecRecord1 r2 = new RecRecord1();
            in.read(r2);
            istream.close();
            assertTrue(r1.equals(r2));
        } catch (IOException ex) {
            ex.printStackTrace();
        } 
    }
    
    public void testCsv() {
        File tmpfile;
        try {
            tmpfile = new File("/temp/hadooptmp.txt");
            RecRecord1 r1 = new RecRecord1();
            r1.setBoolVal(true);
            r1.setByteVal((byte)0x66);
            r1.setFloatVal(3.145F);
            r1.setDoubleVal(1.5234);
            r1.setIntVal(4567);
            r1.setLongVal(0x5a5a5a5a5a5aL);
            r1.setStringVal(new Text("random text"));
            r1.setBufferVal(new ByteArrayOutputStream(20));
            r1.setVectorVal(new ArrayList());
            r1.setMapVal(new TreeMap());
            FileInputStream istream = new FileInputStream(tmpfile);
            RecordReader in = new RecordReader(istream, "csv");
            RecRecord1 r2 = new RecRecord1();
            in.read(r2);
            istream.close();
            assertTrue(r1.equals(r2));
        } catch (IOException ex) {
            ex.printStackTrace();
        } 
    }

    public void testXml() {
        File tmpfile;
        try {
            tmpfile = new File("/temp/hadooptmp.xml");
            RecRecord1 r1 = new RecRecord1();
            r1.setBoolVal(true);
            r1.setByteVal((byte)0x66);
            r1.setFloatVal(3.145F);
            r1.setDoubleVal(1.5234);
            r1.setIntVal(4567);
            r1.setLongVal(0x5a5a5a5a5a5aL);
            r1.setStringVal(new Text("random text"));
            r1.setBufferVal(new ByteArrayOutputStream(20));
            r1.setVectorVal(new ArrayList());
            r1.setMapVal(new TreeMap());
            FileInputStream istream = new FileInputStream(tmpfile);
            RecordReader in = new RecordReader(istream, "xml");
            RecRecord1 r2 = new RecRecord1();
            in.read(r2);
            istream.close();
            assertTrue(r1.equals(r2));
        } catch (IOException ex) {
            ex.printStackTrace();
        } 
    }

}
