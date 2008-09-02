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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;


import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde.SerDe;
import org.apache.hadoop.hive.serde.SerDeException;

/**
 * File Sink operator implementation
 **/
public class FileSinkOperator extends TerminalOperator <fileSinkDesc> implements Serializable {

  public static interface RecordWriter {
    public void write(Writable w) throws IOException;
    public void close(boolean abort) throws IOException;
  }

  private static final long serialVersionUID = 1L;
  transient protected RecordWriter outWriter;
  transient protected FileSystem fs;
  transient protected Path outPath;
  transient protected Path finalPath;
  transient protected SerDe serDe;
  transient protected BytesWritable commonKey = new BytesWritable();
  
  private void commit() throws IOException {
    fs.rename(outPath, finalPath);
  }

  public void close(boolean abort) throws HiveException {
    if(!abort) {
      if (outWriter != null) {
        try {
          outWriter.close(abort);
          commit();
        } catch (IOException e) {
          throw new HiveException("Error in committing output in file: "+ outPath.toString());
        }
      }
    } else {
      try {
        outWriter.close(abort);
        fs.delete(outPath, true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    try {
      fs = FileSystem.get(hconf);
      finalPath = new Path(conf.getDirName(), Utilities.getTaskId(hconf));
      outPath = new Path(conf.getDirName(), "tmp."+Utilities.getTaskId(hconf));
      OutputFormat outputFormat = conf.getTableInfo().getOutputFileFormatClass().newInstance();

      if(outputFormat instanceof IgnoreKeyTextOutputFormat) {
        final FSDataOutputStream outStream = fs.create(outPath);
        outWriter = new RecordWriter () {
            public void write(Writable r) throws IOException {
              Text tr = (Text)r;
              outStream.write(tr.getBytes(), 0, tr.getLength());
              outStream.write('\n');
            }
            public void close(boolean abort) throws IOException {
              outStream.close();
            }
          };
      } else if (outputFormat instanceof SequenceFileOutputFormat) {
        final SequenceFile.Writer outStream =
          SequenceFile.createWriter(fs, hconf, outPath, BytesWritable.class, Text.class);
        outWriter = new RecordWriter () {
            public void write(Writable r) throws IOException {
              outStream.append(commonKey, r);
            }
            public void close(boolean abort) throws IOException {
              outStream.close();
            }
          };
      } else {
        // should never come here - we should be catching this in ddl command
        assert(false);
      }
      serDe = conf.getTableInfo().getSerdeClass().newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  public void process(HiveObject r) throws HiveException {
    try {
      // user SerDe to serialize r, and write it out
      Writable value = serDe.serialize(r.getJavaObject());
      outWriter.write(value);
    } catch (IOException e) {
      throw new HiveException (e);
    } catch (SerDeException e) {
      throw new HiveException (e);
    }
  }
}
