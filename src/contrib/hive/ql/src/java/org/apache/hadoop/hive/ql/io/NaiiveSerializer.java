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

package org.apache.hadoop.hive.ql.io;

import java.util.*;
import java.io.*;

import org.apache.hadoop.hive.ql.exec.HiveObject;
import org.apache.hadoop.hive.ql.exec.CompositeHiveObject;
import org.apache.hadoop.hive.ql.exec.PrimitiveHiveObject;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.utils.ByteStream;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Serializes and deserializes Hive Objects as a delimited strings.
 **/
public class NaiiveSerializer implements HiveObjectSerializer {

  List<SerDeField> topLevelFields;
  NaiiveSerializer [] topLevelSerializers;

  int separator = Utilities.ctrlaCode;
  int terminator = Utilities.newLineCode;
  byte[] nullByteArray;

  long writeErrorCount = 0, readErrorCount = 0;
  ByteStream.Output bos = new ByteStream.Output ();
  int width = -1;
  ArrayList<String> slist = new ArrayList<String> ();
  boolean isPrimitive, isTopLevel = true;

  private void setSeparator (int separator) {
    this.separator = separator;
  }

  private void setIsTopLevel(boolean value) {
    isTopLevel = value;
  }

  private void setNullByteArray(byte[] nullByteArray) {
    this.nullByteArray = nullByteArray;
  }

  public NaiiveSerializer () {
    try {
      setNullByteArray(Utilities.nullStringStorage.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 should be supported", e);
    }
  }

  public void initialize (Properties p) {
    String separator = p.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT);
    if(separator != null) {
      setSeparator(Integer.parseInt(separator));
    }
    // Will make this configurable when DDL (CREATE TABLE) supports customized null string.
    String nullString = null;
    try {
      if (nullString != null) {
        setNullByteArray(nullString.getBytes("UTF-8"));
      } else {
        setNullByteArray(Utilities.nullStringStorage.getBytes("UTF-8"));
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 should be supported", e);
    }
  }

  public void serialize(HiveObject ho, DataOutput os) throws IOException {
    try {
      if (ho.getIsNull()) {
        os.write(nullByteArray);
      } else {
        if(topLevelFields == null) {
          try {
            if(ho.isPrimitive()) {
              topLevelFields = HiveObject.nlist;
              isPrimitive = true;
            } else {
              topLevelFields = ho.getFields();
              isPrimitive = false;
              assert(topLevelFields != null);
              topLevelSerializers = new NaiiveSerializer [topLevelFields.size()];
              for(int i=0; i<topLevelFields.size(); i++) {
                topLevelSerializers[i] = new NaiiveSerializer();
                topLevelSerializers[i].setSeparator(separator+1);
                topLevelSerializers[i].setIsTopLevel(false);
                topLevelSerializers[i].setNullByteArray(nullByteArray);
              }

              //System.err.println("Naiive: Hive Object has "+topLevelFields.size()+" fields");
            }
          } catch (HiveException e) {
            throw new RuntimeException ("Cannot get Fields from HiveObject", e);
          }
        }
  
        if(isPrimitive) {
          os.write(ho.getJavaObject().toString().getBytes("UTF-8"));
        } else {
          boolean first = true;
          int i = -1;
          for(SerDeField onef: topLevelFields) {
            i++;
  
            if(!first) {
              os.write(separator);
            } else {
              first = false;
            }
            HiveObject nho = ho.get(onef);
            if(nho == null)
              continue;
  
            if(nho.isPrimitive()) {
              os.write(nho.getJavaObject().toString().getBytes("UTF-8"));
            } else {
              topLevelSerializers[i].serialize(nho, os);
            }
          }
        }
      }
      
      if(isTopLevel) {
        os.write(terminator);
      }
    } catch (HiveException e) {
      writeErrorCount++;
    }
  }

  private final static String NSTR = "";
  private static enum streamStatus {EOF, TERMINATED, NORMAL}
  public HiveObject deserialize (DataInput in) throws IOException {
    boolean more = true;
    CompositeHiveObject nr = null;
    int entries = 0;

    if(width != -1) {
      nr = new CompositeHiveObject (width);
    } else {
      slist.clear();
    }

    do {
      bos.reset();
      streamStatus ss = readColumn(in, bos);
      if((ss == streamStatus.EOF) ||
         (ss == streamStatus.TERMINATED)) {
        // read off entire row/file
        more = false;
      }

      entries ++;
      String col;
      if(bos.getCount() > 0) {
        col = new String(bos.getData(), 0, bos.getCount(), "UTF-8");
      } else {
        col = NSTR;
      }

      if(width == -1) {
        slist.add(col);
      } else {
        if(entries <= width) {
          try {
            nr.addHiveObject(new PrimitiveHiveObject(col));
          } catch (HiveException e) {
            e.printStackTrace();
            throw new IOException (e.getMessage());
          }
        }
      }
    } while (more);

    if (width == -1) {
      width = entries;
      nr = new CompositeHiveObject (width);
      for(String col: slist) {
        try {
          nr.addHiveObject(new PrimitiveHiveObject(col));
        } catch (HiveException e) {
          e.printStackTrace();
          throw new IOException (e.getMessage());
        }
      }
      return (nr);
    }

    if(width > entries) {
      // skip and move on ..
      readErrorCount++;
      return null;
    } else {
      return nr;
    }
  }

  public long getReadErrorCount() {
    return readErrorCount;
  }

  public long getWriteErrorCount() {
    return writeErrorCount;
  }

  private streamStatus readColumn(DataInput in, OutputStream out) throws IOException {
    while (true) {
      int b;
      try {
        b = (int)in.readByte();
      } catch (EOFException e) {
        return streamStatus.EOF;
      }

      if (b == terminator) {
        return streamStatus.TERMINATED;
      }

      if (b == separator) {
        return streamStatus.NORMAL;
      }

      out.write(b);
    }
    // Unreachable
  }

  public int compare(byte [] b1, int s1, int l1, byte [] b2, int s2, int l2) {
    // Since all data is strings - we just use lexicographic ordering
    return WritableComparator.compareBytes(b1, s1, l2, b2, s2, l2);
  }
}
