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
package org.apache.hadoop.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class TestLogalyzer {
  private static String EL = System.getProperty("line.separator");
  private static String TAB = "\t";
  private static final Log LOG = LogFactory.getLog(TestLogalyzer.class);

  private static File workSpace = new File("target",
      TestLogalyzer.class.getName() + "-workSpace");
  private static File outdir = new File(workSpace.getAbsoluteFile()
      + File.separator + "out");

  @Test
  public void testLogalyzer() throws Exception {
    Path f = createLogFile();

    String[] args = new String[10];

    args[0] = "-archiveDir";
    args[1] = f.toString();
    args[2] = "-grep";
    args[3] = "44";
    args[4] = "-sort";
    args[5] = "0";
    args[6] = "-analysis";
    args[7] = outdir.getAbsolutePath();
    args[8] = "-separator";
    args[9] = " ";

    Logalyzer.main(args);
    checkResult();

  }

  private void checkResult() throws Exception {
    File result = new File(outdir.getAbsolutePath() + File.separator
        + "part-00000");
    File success = new File(outdir.getAbsolutePath() + File.separator
        + "_SUCCESS");
    Assert.assertTrue(success.exists());

    FileInputStream fis = new FileInputStream(result);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
    String line = br.readLine();
    Assert.assertTrue(("1 44" + TAB + "2").equals(line));
    line = br.readLine();

    Assert.assertTrue(("3 44" + TAB + "1").equals(line));
    line = br.readLine();

    Assert.assertTrue(("4 44" + TAB + "1").equals(line));

    br.close();

  }

  /**
   * Create simple log file
   * 
   * @return
   * @throws IOException
   */

  private Path createLogFile() throws IOException {

    FileContext files = FileContext.getLocalFSFileContext();

    Path ws = new Path(workSpace.getAbsoluteFile().getAbsolutePath());

    files.delete(ws, true);
    Path workSpacePath = new Path(workSpace.getAbsolutePath(), "log");
    files.mkdir(workSpacePath, null, true);

    LOG.info("create logfile.log");
    Path logfile1 = new Path(workSpacePath, "logfile.log");

    FSDataOutputStream os = files.create(logfile1,
        EnumSet.of(CreateFlag.CREATE));
    os.writeBytes("4 3" + EL + "1 3" + EL + "4 44" + EL);
    os.writeBytes("2 3" + EL + "1 3" + EL + "0 45" + EL);
    os.writeBytes("4 3" + EL + "1 3" + EL + "1 44" + EL);

    os.flush();
    os.close();
    LOG.info("create logfile1.log");

    Path logfile2 = new Path(workSpacePath, "logfile1.log");

    os = files.create(logfile2, EnumSet.of(CreateFlag.CREATE));
    os.writeBytes("4 3" + EL + "1 3" + EL + "3 44" + EL);
    os.writeBytes("2 3" + EL + "1 3" + EL + "0 45" + EL);
    os.writeBytes("4 3" + EL + "1 3" + EL + "1 44" + EL);

    os.flush();
    os.close();

    return workSpacePath;
  }
}
