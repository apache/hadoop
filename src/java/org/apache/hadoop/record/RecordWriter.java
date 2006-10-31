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

import java.io.IOException;
import java.io.OutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

/**
 * Front-end for serializers. Also serves as a factory for serializers.
 *
 * @author Milind Bhandarkar
 */
public class RecordWriter {
    
    private OutputArchive archive;
    
    static private OutputArchive getBinaryArchive(OutputStream out) {
        return new BinaryOutputArchive(new DataOutputStream(out));
    }
    
    static private OutputArchive getCsvArchive(OutputStream out)
    throws IOException {
        try {
            return new CsvOutputArchive(out);
        } catch (UnsupportedEncodingException ex) {
            throw new IOException("Unsupported encoding UTF-8");
        }
    }
    
    static private OutputArchive getXmlArchive(OutputStream out)
    throws IOException {
        return new XmlOutputArchive(out);
    }

    static HashMap constructFactory() {
        HashMap factory = new HashMap();
        Class[] params = { OutputStream.class };
        try {
            factory.put("binary",
                    BinaryOutputArchive.class.getDeclaredMethod(
                        "getArchive", params));
            factory.put("csv",
                    CsvOutputArchive.class.getDeclaredMethod(
                        "getArchive", params));
            factory.put("xml",
                    XmlOutputArchive.class.getDeclaredMethod(
                        "getArchive", params));
        } catch (SecurityException ex) {
            ex.printStackTrace();
        } catch (NoSuchMethodException ex) {
            ex.printStackTrace();
        }
        return factory;
    }
    
    static private HashMap archiveFactory = constructFactory();
    
    static private OutputArchive createArchive(OutputStream out,
            String format)
            throws IOException {
        Method factory = (Method) archiveFactory.get(format);
        if (factory != null) {
            Object[] params = { out };
            try {
                return (OutputArchive) factory.invoke(null, params);
            } catch (IllegalArgumentException ex) {
                ex.printStackTrace();
            } catch (InvocationTargetException ex) {
                ex.printStackTrace();
            } catch (IllegalAccessException ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }
    /**
     * Creates a new instance of RecordWriter
     * @param out Output stream where the records will be serialized
     * @param format Serialization format ("binary", "xml", or "csv")
     */
    public RecordWriter(OutputStream out, String format)
    throws IOException {
        archive = createArchive(out, format);
    }
    
    /**
     * Serialize a record
     * @param r record to be serialized
     */
    public void write(Record r) throws IOException {
        r.serialize(archive, "");
    }
}
