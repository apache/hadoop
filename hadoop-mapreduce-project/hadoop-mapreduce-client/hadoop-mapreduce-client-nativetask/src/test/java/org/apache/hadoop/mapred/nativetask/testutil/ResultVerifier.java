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
package org.apache.hadoop.mapred.nativetask.testutil;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;

public class ResultVerifier {
  /**
   * verify the result
   * 
   * @param sample the path to correct results
   * @param source the path to the results from the native implementation
   */
  public static boolean verify(String sample, String source) throws Exception {
    FSDataInputStream sourcein = null;
    FSDataInputStream samplein = null;

    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    final Path hdfssource = new Path(source);
    final Path[] sourcepaths = FileUtil.stat2Paths(fs.listStatus(hdfssource));

    final Path hdfssample = new Path(sample);
    final Path[] samplepaths = FileUtil.stat2Paths(fs.listStatus(hdfssample));
    if (sourcepaths == null) {
      throw new Exception("source file can not be found");
    }
    if (samplepaths == null) {
      throw new Exception("sample file can not be found");
    }
    if (sourcepaths.length != samplepaths.length) {
      return false;
    }
    for (int i = 0; i < sourcepaths.length; i++) {
      final Path sourcepath = sourcepaths[i];
      // op result file start with "part-r" like part-r-00000

      if (!sourcepath.getName().startsWith("part-r")) {
        continue;
      }
      Path samplepath = null;
      for (int j = 0; j < samplepaths.length; j++) {
        if (samplepaths[i].getName().equals(sourcepath.getName())) {
          samplepath = samplepaths[i];
          break;
        }
      }
      if (samplepath == null) {
        throw new Exception("cound not find file " +
                            samplepaths[0].getParent() + "/" + sourcepath.getName()
                            + " , as sourcepaths has such file");
      }

      // compare
      try {
        if (fs.exists(sourcepath) && fs.exists(samplepath)) {
          sourcein = fs.open(sourcepath);
          samplein = fs.open(samplepath);
        } else {
          System.err.println("result file not found:" + sourcepath + " or " + samplepath);
          return false;
        }

        CRC32 sourcecrc, samplecrc;
        samplecrc = new CRC32();
        sourcecrc = new CRC32();
        final byte[] bufin = new byte[1 << 16];
        int readnum = 0;
        int totalRead = 0;
        while (samplein.available() > 0) {
          readnum = samplein.read(bufin);
          totalRead += readnum;
          samplecrc.update(bufin, 0, readnum);
        }

        if (0 == totalRead) {
          throw new Exception("source " + sample + " is empty file");
        }

        totalRead = 0;
        while (sourcein.available() > 0) {
          readnum = sourcein.read(bufin);
          totalRead += readnum;
          sourcecrc.update(bufin, 0, readnum);
        }
        if (0 == totalRead) {
          throw new Exception("source " + sample + " is empty file");
        }

        if (samplecrc.getValue() == sourcecrc.getValue()) {
          ;
        } else {
          return false;
        }
      } catch (final IOException e) {
        throw new Exception("verify exception :", e);
      } finally {

        try {
          if (samplein != null) {
            samplein.close();
          }
          if (sourcein != null) {
            sourcein.close();
          }
        } catch (final IOException e) {
          e.printStackTrace();
        }

      }
    }
    return true;
  }

  public static void verifyCounters(Job normalJob, Job nativeJob, boolean hasCombiner)
      throws IOException {
    Counters normalCounters = normalJob.getCounters();
    Counters nativeCounters = nativeJob.getCounters();
    assertEquals("Counter MAP_OUTPUT_RECORDS should be equal",
        normalCounters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue(),
        nativeCounters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
    assertEquals("Counter REDUCE_INPUT_GROUPS should be equal",
        normalCounters.findCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue(),
        nativeCounters.findCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue());
    if (!hasCombiner) {
      assertEquals("Counter REDUCE_INPUT_RECORDS should be equal",
          normalCounters.findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue(),
          nativeCounters.findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue());
    }
  }

  public static void verifyCounters(Job normalJob, Job nativeJob) throws IOException {
    verifyCounters(normalJob, nativeJob, false);
  }
}
