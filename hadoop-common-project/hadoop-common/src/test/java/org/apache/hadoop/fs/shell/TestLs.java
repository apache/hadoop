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
package org.apache.hadoop.fs.shell;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;

/**
 * JUnit test class for {@link org.apache.hadoop.fs.shell.Ls}
 *
 */
public class TestLs {
  private static Configuration conf;
  private static FileSystem mockFs;

  private static final Date NOW = new Date();

  @BeforeClass
  public static void setup() throws IOException {
    conf = new Configuration();
    conf.set("fs.defaultFS", "mockfs:///");
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    mockFs = mock(FileSystem.class);
  }

  @Before
  public void resetMock() throws IOException {
    reset(mockFs);
    AclStatus mockAclStatus = mock(AclStatus.class);
    when(mockAclStatus.getEntries()).thenReturn(new ArrayList<AclEntry>());
    when(mockFs.getAclStatus(any(Path.class))).thenReturn(mockAclStatus);
  }

  // check that default options are correct
  @Test
  public void processOptionsNone() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertFalse(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // check the -C option is recognised
  @Test
  public void processOptionsPathOnly() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-C");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertTrue(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertFalse(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // check the -d option is recognised
  @Test
  public void processOptionsDirectory() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-d");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertFalse(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertFalse(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // check the -h option is recognised
  @Test
  public void processOptionsHuman() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-h");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertTrue(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertFalse(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // check the -R option is recognised
  @Test
  public void processOptionsRecursive() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-R");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertTrue(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertFalse(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // check the -r option is recognised
  @Test
  public void processOptionsReverse() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-r");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertTrue(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertFalse(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // check the -S option is recognised
  @Test
  public void processOptionsSize() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-S");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertTrue(ls.isOrderSize());
    assertFalse(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // check the -t option is recognised
  @Test
  public void processOptionsMtime() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-t");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertTrue(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // check the precedence of the -t and -S options
  @Test
  public void processOptionsMtimeSize() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-t");
    options.add("-S");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertTrue(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // check the precedence of the -t, -S and -r options
  @Test
  public void processOptionsMtimeSizeReverse() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-t");
    options.add("-S");
    options.add("-r");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertTrue(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertTrue(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  // chheck the -u option is recognised
  @Test
  public void processOptionsAtime() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-u");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertFalse(ls.isOrderTime());
    assertTrue(ls.isUseAtime());
  }

  // check all options is handled correctly
  @Test
  public void processOptionsAll() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-C"); // show file path only
    options.add("-d"); // directory
    options.add("-h"); // human readable
    options.add("-R"); // recursive
    options.add("-r"); // reverse order
    options.add("-t"); // time order
    options.add("-S"); // size order
    options.add("-u"); // show atime
    Ls ls = new Ls();
    ls.processOptions(options);
    assertTrue(ls.isPathOnly());
    assertFalse(ls.isDirRecurse());
    assertTrue(ls.isHumanReadable());
    assertFalse(ls.isRecursive()); // -d overrules -R
    assertTrue(ls.isOrderReverse());
    assertFalse(ls.isOrderSize()); // -t overrules -S
    assertTrue(ls.isOrderTime());
    assertTrue(ls.isUseAtime());
  }

  // check listing of a single file
  @Test
  public void processPathFile() throws IOException {
    TestFile testfile = new TestFile("testDir", "testFile");

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testfile.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println(testfile.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check listing of multiple files
  @Test
  public void processPathFiles() throws IOException {
    TestFile testfile01 = new TestFile("testDir01", "testFile01");
    TestFile testfile02 = new TestFile("testDir02", "testFile02");
    TestFile testfile03 = new TestFile("testDir03", "testFile03");
    TestFile testfile04 = new TestFile("testDir04", "testFile04");
    TestFile testfile05 = new TestFile("testDir05", "testFile05");
    TestFile testfile06 = new TestFile("testDir06", "testFile06");

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testfile01.getPathData());
    pathData.add(testfile02.getPathData());
    pathData.add(testfile03.getPathData());
    pathData.add(testfile04.getPathData());
    pathData.add(testfile05.getPathData());
    pathData.add(testfile06.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check listing of a single directory
  @Test
  public void processPathDirectory() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check listing of multiple directories
  @Test
  public void processPathDirectories() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory01", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory01", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory01", "testFile03");

    TestFile testDir01 = new TestFile("", "testDirectory01");
    testDir01.setIsDir(true);
    testDir01.addContents(testfile01, testfile02, testfile03);

    TestFile testfile04 = new TestFile("testDirectory02", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory02", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory02", "testFile06");
    TestFile testDir02 = new TestFile("", "testDirectory02");
    testDir02.setIsDir(true);
    testDir02.addContents(testfile04, testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir01.getPathData());
    pathData.add(testDir02.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 3 items");
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println("Found 3 items");
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check the default ordering
  @Test
  public void processPathDirOrderDefault() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    // add contents in non-lexigraphic order to show they get sorted
    testDir.addContents(testfile01, testfile03, testfile05, testfile02,
        testfile04, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check reverse default ordering
  @Test
  public void processPathDirOrderDefaultReverse() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    // add contents in non-lexigraphic order to show they get sorted
    testDir.addContents(testfile01, testfile03, testfile05, testfile02,
        testfile04, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-r");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check mtime ordering (-t option); most recent first in line with unix
  // convention
  @Test
  public void processPathDirOrderMtime() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    // set file mtime in different order to file names
    testfile01.setMtime(NOW.getTime() + 10);
    testfile02.setMtime(NOW.getTime() + 30);
    testfile03.setMtime(NOW.getTime() + 20);
    testfile04.setMtime(NOW.getTime() + 60);
    testfile05.setMtime(NOW.getTime() + 50);
    testfile06.setMtime(NOW.getTime() + 40);

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-t");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check reverse mtime ordering (-t -r options)
  @Test
  public void processPathDirOrderMtimeReverse() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    // set file mtime in different order to file names
    testfile01.setMtime(NOW.getTime() + 10);
    testfile02.setMtime(NOW.getTime() + 30);
    testfile03.setMtime(NOW.getTime() + 20);
    testfile04.setMtime(NOW.getTime() + 60);
    testfile05.setMtime(NOW.getTime() + 50);
    testfile06.setMtime(NOW.getTime() + 40);

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-t");
    options.add("-r");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check multiple directories are order independently
  @Test
  public void processPathDirsOrderMtime() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory01", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory01", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory01", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory02", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory02", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory02", "testFile06");

    // set file mtime in different order to file names
    testfile01.setMtime(NOW.getTime() + 10);
    testfile02.setMtime(NOW.getTime() + 30);
    testfile03.setMtime(NOW.getTime() + 20);
    testfile04.setMtime(NOW.getTime() + 60);
    testfile05.setMtime(NOW.getTime() + 40);
    testfile06.setMtime(NOW.getTime() + 50);

    TestFile testDir01 = new TestFile("", "testDirectory01");
    testDir01.setIsDir(true);
    testDir01.addContents(testfile01, testfile02, testfile03);

    TestFile testDir02 = new TestFile("", "testDirectory02");
    testDir02.setIsDir(true);
    testDir02.addContents(testfile04, testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir01.getPathData());
    pathData.add(testDir02.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-t");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 3 items");
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    inOrder.verify(out).println("Found 3 items");
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check mtime ordering with large time gaps between files (checks integer
  // overflow issues)
  @Test
  public void processPathDirOrderMtimeYears() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    // set file mtime in different order to file names
    testfile01.setMtime(NOW.getTime() + Integer.MAX_VALUE);
    testfile02.setMtime(NOW.getTime() + Integer.MIN_VALUE);
    testfile03.setMtime(NOW.getTime() + 0);
    testfile04.setMtime(NOW.getTime() + Integer.MAX_VALUE + Integer.MAX_VALUE);
    testfile05.setMtime(NOW.getTime() + 0);
    testfile06.setMtime(NOW.getTime() + Integer.MIN_VALUE + Integer.MIN_VALUE);

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-t");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check length order (-S option)
  @Test
  public void processPathDirOrderLength() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    // set file length in different order to file names
    long length = 1234567890;
    testfile01.setLength(length + 10);
    testfile02.setLength(length + 30);
    testfile03.setLength(length + 20);
    testfile04.setLength(length + 60);
    testfile05.setLength(length + 50);
    testfile06.setLength(length + 40);

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-S");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check reverse length order (-S -r options)
  @Test
  public void processPathDirOrderLengthReverse() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    // set file length in different order to file names
    long length = 1234567890;
    testfile01.setLength(length + 10);
    testfile02.setLength(length + 30);
    testfile03.setLength(length + 20);
    testfile04.setLength(length + 60);
    testfile05.setLength(length + 50);
    testfile06.setLength(length + 40);

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-S");
    options.add("-r");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check length ordering with large size gaps between files (checks integer
  // overflow issues)
  @Test
  public void processPathDirOrderLengthLarge() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    // set file length in different order to file names
    long length = 1234567890;
    testfile01.setLength(length + 3l * Integer.MAX_VALUE);
    testfile02.setLength(length + Integer.MAX_VALUE);
    testfile03.setLength(length + 2l * Integer.MAX_VALUE);
    testfile04.setLength(length + 4l * Integer.MAX_VALUE);
    testfile05.setLength(length + 2l * Integer.MAX_VALUE);
    testfile06.setLength(length + 0);

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-S");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile04.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile01.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineMtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineMtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check access time display (-u option)
  @Test
  public void processPathDirectoryAtime() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-u");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile01.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile04.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineAtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check access time order (-u -t options)
  @Test
  public void processPathDirOrderAtime() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    // set file atime in different order to file names
    testfile01.setAtime(NOW.getTime() + 10);
    testfile02.setAtime(NOW.getTime() + 30);
    testfile03.setAtime(NOW.getTime() + 20);
    testfile04.setAtime(NOW.getTime() + 60);
    testfile05.setAtime(NOW.getTime() + 50);
    testfile06.setAtime(NOW.getTime() + 40);

    // set file mtime in different order to atime
    testfile01.setMtime(NOW.getTime() + 60);
    testfile02.setMtime(NOW.getTime() + 50);
    testfile03.setMtime(NOW.getTime() + 20);
    testfile04.setMtime(NOW.getTime() + 30);
    testfile05.setMtime(NOW.getTime() + 10);
    testfile06.setMtime(NOW.getTime() + 40);

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-t");
    options.add("-u");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile04.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile01.formatLineAtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check reverse access time order (-u -t -r options)
  @Test
  public void processPathDirOrderAtimeReverse() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    // set file atime in different order to file names
    testfile01.setAtime(NOW.getTime() + 10);
    testfile02.setAtime(NOW.getTime() + 30);
    testfile03.setAtime(NOW.getTime() + 20);
    testfile04.setAtime(NOW.getTime() + 60);
    testfile05.setAtime(NOW.getTime() + 50);
    testfile06.setAtime(NOW.getTime() + 40);

    // set file mtime in different order to atime
    testfile01.setMtime(NOW.getTime() + 60);
    testfile02.setMtime(NOW.getTime() + 50);
    testfile03.setMtime(NOW.getTime() + 20);
    testfile04.setMtime(NOW.getTime() + 30);
    testfile05.setMtime(NOW.getTime() + 10);
    testfile06.setMtime(NOW.getTime() + 40);

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-t");
    options.add("-u");
    options.add("-r");
    ls.processOptions(options);
    String lineFormat = TestFile.computeLineFormat(pathData);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println("Found 6 items");
    inOrder.verify(out).println(testfile01.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile03.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile02.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile06.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile05.formatLineAtime(lineFormat));
    inOrder.verify(out).println(testfile04.formatLineAtime(lineFormat));
    verifyNoMoreInteractions(out);
  }

  // check path only display (-C option)
  @Test
  public void processPathDirectoryPathOnly() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-C");
    ls.processOptions(options);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println(testfile01.getPath().toString());
    inOrder.verify(out).println(testfile02.getPath().toString());
    inOrder.verify(out).println(testfile03.getPath().toString());
    inOrder.verify(out).println(testfile04.getPath().toString());
    inOrder.verify(out).println(testfile05.getPath().toString());
    inOrder.verify(out).println(testfile06.getPath().toString());
    verifyNoMoreInteractions(out);
  }

  private static void displayWarningOnLocalFileSystem(boolean shouldDisplay)
      throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(
        HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY, shouldDisplay);

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(buf, true);
    Ls ls = new Ls(conf);
    ls.err = err;
    ls.run("file:///.");
    assertEquals(shouldDisplay, buf.toString().contains(
        "Warning: fs.defaultFS is not set when running \"ls\" command."));
  }

  @Test
  public void displayWarningsOnLocalFileSystem() throws IOException {
    // Display warnings.
    displayWarningOnLocalFileSystem(true);
    // Does not display warnings.
    displayWarningOnLocalFileSystem(false);
  }

  // check the deprecated flag isn't set
  @Test
  public void isDeprecated() {
    Ls ls = new Ls();
    boolean actual = ls.isDeprecated();
    boolean expected = false;
    assertEquals("Ls.isDeprecated", expected, actual);
  }

  // check there's no replacement command
  @Test
  public void getReplacementCommand() {
    Ls ls = new Ls();
    String actual = ls.getReplacementCommand();
    String expected = null;
    assertEquals("Ls.getReplacementCommand", expected, actual);
  }

  // check the correct name is returned
  @Test
  public void getName() {
    Ls ls = new Ls();
    String actual = ls.getName();
    String expected = "ls";
    assertEquals("Ls.getName", expected, actual);
  }

  // test class representing a file to be listed
  static class TestFile {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
        "yyyy-MM-dd HH:mm");

    private static final boolean DEFAULT_ISDIR = false;
    private static final String DEFAULT_MODE = "750";
    private static final int DEFAULT_REPLICATION = 3;
    private static final String DEFAULT_OWNER = "test_owner";
    private static final String DEFAULT_GROUP = "test_group";
    private static final long DEFAULT_LENGTH = 1234567890L;
    private static final long DEFAULT_MTIME = NOW.getTime() - 86400000;
    private static final long DEFAULT_ATIME = NOW.getTime() + 86400000;
    private static final long DEFAULT_BLOCKSIZE = 64L * 1024 * 1024;

    private String dirname;
    private String filename;
    private boolean isDir;
    private FsPermission permission;
    private int replication;
    private String owner;
    private String group;
    private long length;
    private long mtime;
    private long atime;
    private long blocksize;
    private ArrayList<FileStatus> contents = new ArrayList<FileStatus>();

    private Path path = null;
    private FileStatus fileStatus = null;
    private PathData pathData = null;

    public TestFile(String dirname, String filename) {
      setDirname(dirname);
      setFilename(filename);
      setIsDir(DEFAULT_ISDIR);
      setPermission(DEFAULT_MODE);
      setReplication(DEFAULT_REPLICATION);
      setOwner(DEFAULT_OWNER);
      setGroup(DEFAULT_GROUP);
      setLength(DEFAULT_LENGTH);
      setMtime(DEFAULT_MTIME);
      setAtime(DEFAULT_ATIME);
      setBlocksize(DEFAULT_BLOCKSIZE);
    }

    public void setDirname(String dirname) {
      this.dirname = dirname;
    }

    public void setFilename(String filename) {
      this.filename = filename;
    }

    public void setIsDir(boolean isDir) {
      this.isDir = isDir;
    }

    public void setPermission(String mode) {
      setPermission(new FsPermission(mode));
    }

    public void setPermission(FsPermission permission) {
      this.permission = permission;
    }

    public void setReplication(int replication) {
      this.replication = replication;
    }

    public void setOwner(String owner) {
      this.owner = owner;
    }

    public void setGroup(String group) {
      this.group = group;
    }

    public void setLength(long length) {
      this.length = length;
    }

    public void setMtime(long mtime) {
      this.mtime = mtime;
    }

    public void setAtime(long atime) {
      this.atime = atime;
    }

    public void setBlocksize(long blocksize) {
      this.blocksize = blocksize;
    }

    public void addContents(TestFile... contents) {
      for (TestFile testFile : contents) {
        this.contents.add(testFile.getFileStatus());
      }
    }

    private String getDirname() {
      return this.dirname;
    }

    private String getFilename() {
      return this.filename;
    }

    private String getPathname() {
      return getDirname() + "/" + getFilename();
    }

    private boolean isDir() {
      return this.isDir;
    }

    private boolean isFile() {
      return !this.isDir();
    }

    private FsPermission getPermission() {
      return this.permission;
    }

    private int getReplication() {
      return this.replication;
    }

    private String getOwner() {
      return this.owner;
    }

    private String getGroup() {
      return this.group;
    }

    private long getLength() {
      return this.length;
    }

    private long getMtime() {
      return this.mtime;
    }

    private long getAtime() {
      return this.atime;
    }

    private long getBlocksize() {
      return this.blocksize;
    }

    private FileStatus[] getContents() {
      return this.contents.toArray(new FileStatus[0]);
    }

    /**
     * Returns a formated output line based on the given format mask, file
     * status and file name.
     *
     * @param lineFormat
     *          format mask
     * @param fileStatus
     *          file status
     * @param fileName
     *          file name
     * @return formated line
     */
    private String formatLineMtime(String lineFormat) {
      return String.format(lineFormat, (isDir() ? "d" : "-"), getPermission(),
          (isFile() ? getReplication() : "-"), getOwner(), getGroup(),
          String.valueOf(getLength()),
          DATE_FORMAT.format(new Date(getMtime())), getPathname());
    }

    /**
     * Returns a formated output line based on the given format mask, file
     * status and file name.
     *
     * @param lineFormat
     *          format mask
     * @param fileStatus
     *          file status
     * @param fileName
     *          file name
     * @return formated line
     */
    private String formatLineAtime(String lineFormat) {
      return String.format(lineFormat, (isDir() ? "d" : "-"), getPermission(),
          (isFile() ? getReplication() : "-"), getOwner(), getGroup(),
          String.valueOf(getLength()),
          DATE_FORMAT.format(new Date(getAtime())), getPathname());
    }

    public FileStatus getFileStatus() {
      if (fileStatus == null) {
        Path path = getPath();
        fileStatus = new FileStatus(getLength(), isDir(), getReplication(),
            getBlocksize(), getMtime(), getAtime(), getPermission(),
            getOwner(), getGroup(), path);
      }
      return fileStatus;
    }

    public Path getPath() {
      if (path == null) {
        if ((getDirname() != null) && (!getDirname().equals(""))) {
          path = new Path(getDirname(), getFilename());
        } else {
          path = new Path(getFilename());
        }
      }
      return path;
    }

    public PathData getPathData() throws IOException {
      if (pathData == null) {
        FileStatus fileStatus = getFileStatus();
        Path path = getPath();
        when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
        pathData = new PathData(path.toString(), conf);

        if (getContents().length != 0) {
          when(mockFs.listStatus(eq(path))).thenReturn(getContents());
        }

      }
      return pathData;
    }

    /**
     * Compute format string based on maximum column widths. Copied from
     * Ls.adjustColumnWidths as these tests are more interested in proving
     * regression rather than absolute format.
     *
     * @param items
     *          to find the max field width for each column
     */
    public static String computeLineFormat(LinkedList<PathData> items) {
      int maxRepl = 3, maxLen = 10, maxOwner = 0, maxGroup = 0;
      for (PathData item : items) {
        FileStatus stat = item.stat;
        maxRepl = maxLength(maxRepl, stat.getReplication());
        maxLen = maxLength(maxLen, stat.getLen());
        maxOwner = maxLength(maxOwner, stat.getOwner());
        maxGroup = maxLength(maxGroup, stat.getGroup());
      }

      StringBuilder fmt = new StringBuilder();
      fmt.append("%s%s "); // permission string
      fmt.append("%" + maxRepl + "s ");
      // Do not use '%-0s' as a formatting conversion, since it will throw a
      // a MissingFormatWidthException if it is used in String.format().
      // http://docs.oracle.com/javase/1.5.0/docs/api/java/util/Formatter.html#intFlags
      fmt.append((maxOwner > 0) ? "%-" + maxOwner + "s " : "%s");
      fmt.append((maxGroup > 0) ? "%-" + maxGroup + "s " : "%s");
      fmt.append("%" + maxLen + "s ");
      fmt.append("%s %s"); // mod time & path
      return fmt.toString();
    }

    /**
     * Return the maximum of two values, treating null as 0
     *
     * @param n
     *          integer to be compared
     * @param value
     *          value to be compared
     * @return maximum of the two inputs
     */
    private static int maxLength(int n, Object value) {
      return Math.max(n, (value != null) ? String.valueOf(value).length() : 0);
    }
  }

  static class MockFileSystem extends FilterFileSystem {
    Configuration conf;

    MockFileSystem() {
      super(mockFs);
    }

    @Override
    public void initialize(URI uri, Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Path makeQualified(Path path) {
      return path;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }
  }
}
