/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.assertj.core.api.Assertions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test getFileStatus and related listing operations.
 */
public abstract class AbstractContractGetFileStatusTest extends
    AbstractFSContractTestBase {

  private Path testPath;
  private Path target;

  // the tree parameters. Kept small to avoid killing object store test
  // runs too much.

  private static final int TREE_DEPTH = 2;
  private static final int TREE_WIDTH = 3;
  private static final int TREE_FILES = 4;
  private static final int TREE_FILESIZE = 512;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_GETFILESTATUS);

    //delete the test directory
    testPath = path("test");
    target = new Path(testPath, "target");
  }

  @Test
  public void testGetFileStatusNonexistentFile() throws Throwable {
    try {
      FileStatus status = getFileSystem().getFileStatus(target);
      //got here: trouble
      fail("expected a failure, got " + status);
    } catch (FileNotFoundException e) {
      //expected
      handleExpectedException(e);
    }
  }

  @Test
  public void testGetFileStatusRoot() throws Throwable {
    ContractTestUtils.assertIsDirectory(
        getFileSystem().getFileStatus(new Path("/")));
  }

  @Test
  public void testListStatusEmptyDirectory() throws IOException {
    describe("List status on an empty directory");
    Path subfolder = createDirWithEmptySubFolder();
    FileSystem fs = getFileSystem();
    Path path = getContract().getTestPath();
    new TreeScanResults(fs.listStatus(path))
        .assertSizeEquals("listStatus(" + path + ")", 0, 1, 0);
    describe("Test on empty subdirectory");
    new TreeScanResults(fs.listStatus(subfolder))
        .assertSizeEquals("listStatus(empty subfolder)", 0, 0, 0);
  }

  @Test
  public void testListFilesEmptyDirectoryNonrecursive() throws IOException {
    listFilesOnEmptyDir(false);
  }

  @Test
  public void testListFilesEmptyDirectoryRecursive() throws IOException {
    listFilesOnEmptyDir(true);
  }

  /**
   * Call listFiles on an directory with an empty subdir.
   * @param recursive should the list be recursive?
   * @throws IOException IO Problems
   */
  private void listFilesOnEmptyDir(boolean recursive) throws IOException {
    describe("Invoke listFiles(recursive=" + recursive + ")" +
        " on empty directories, expect nothing found");
    FileSystem fs = getFileSystem();
    Path path = getContract().getTestPath();
    fs.delete(path, true);
    Path subfolder = createDirWithEmptySubFolder();
    new TreeScanResults(fs.listFiles(path, recursive))
        .assertSizeEquals("listFiles(test dir, " + recursive + ")", 0, 0, 0);
    describe("Test on empty subdirectory");
    new TreeScanResults(fs.listFiles(subfolder, recursive))
        .assertSizeEquals("listFiles(empty subfolder, " + recursive + ")",
            0, 0, 0);
  }

  @Test
  public void testListLocatedStatusEmptyDirectory() throws IOException {
    describe("Invoke listLocatedStatus() on empty directories;" +
        " expect directories to be found");
    FileSystem fs = getFileSystem();
    Path path = getContract().getTestPath();
    fs.delete(path, true);
    Path subfolder = createDirWithEmptySubFolder();
    new TreeScanResults(fs.listLocatedStatus(path))
      .assertSizeEquals("listLocatedStatus(test dir)", 0, 1, 0);
    describe("Test on empty subdirectory");
    new TreeScanResults(fs.listLocatedStatus(subfolder))
        .assertSizeEquals("listLocatedStatus(empty subfolder)", 0, 0, 0);
  }

  /**
   * All tests cases against complex directories are aggregated into one, so
   * that the setup and teardown costs against object stores can be shared.
   * @throws Throwable
   */
  @Test
  public void testComplexDirActions() throws Throwable {
    TreeScanResults tree = createTestTree();
    checkListStatusStatusComplexDir(tree);
    checkListStatusIteratorComplexDir(tree);
    checkListLocatedStatusStatusComplexDir(tree);
    checkListFilesComplexDirNonRecursive(tree);
    checkListFilesComplexDirRecursive(tree);
  }

  /**
   * Test {@link FileSystem#listStatus(Path)} on a complex
   * directory tree.
   * @param tree directory tree to list.
   * @throws Throwable
   */
  protected void checkListStatusStatusComplexDir(TreeScanResults tree)
      throws Throwable {
    describe("Expect listStatus to list all entries in top dir only");

    FileSystem fs = getFileSystem();
    TreeScanResults listing = new TreeScanResults(
        fs.listStatus(tree.getBasePath()));
    listing.assertSizeEquals("listStatus()", TREE_FILES, TREE_WIDTH, 0);
  }

  /**
   * Test {@link FileSystem#listStatusIterator(Path)} on a complex
   * directory tree.
   * @param tree directory tree to list.
   * @throws Throwable
   */
  protected void checkListStatusIteratorComplexDir(TreeScanResults tree)
          throws Throwable {
    describe("Expect listStatusIterator to list all entries in top dir only");

    FileSystem fs = getFileSystem();
    TreeScanResults listing = new TreeScanResults(
            fs.listStatusIterator(tree.getBasePath()));
    listing.assertSizeEquals("listStatus()", TREE_FILES, TREE_WIDTH, 0);

    List<FileStatus> resWithoutCheckingHasNext =
            iteratorToListThroughNextCallsAlone(fs
                    .listStatusIterator(tree.getBasePath()));

    List<FileStatus> resWithCheckingHasNext = iteratorToList(fs
                    .listStatusIterator(tree.getBasePath()));
    Assertions.assertThat(resWithCheckingHasNext)
            .describedAs("listStatusIterator() should return correct " +
                    "results even if hasNext() calls are not made.")
            .hasSameElementsAs(resWithoutCheckingHasNext);

  }

  /**
   * Test {@link FileSystem#listLocatedStatus(Path)} on a complex
   * directory tree.
   * @param tree directory tree to list.
   * @throws Throwable
   */
  protected void checkListLocatedStatusStatusComplexDir(TreeScanResults tree)
      throws Throwable {
    describe("Expect listLocatedStatus to list all entries in top dir only");
    FileSystem fs = getFileSystem();
    TreeScanResults listing = new TreeScanResults(
         fs.listLocatedStatus(tree.getBasePath()));
    listing.assertSizeEquals("listLocatedStatus()", TREE_FILES, TREE_WIDTH, 0);
    verifyFileStats(fs.listLocatedStatus(tree.getBasePath()));

    // listLocatedStatus and listStatus must return the same files.
    TreeScanResults listStatus = new TreeScanResults(
        fs.listStatus(tree.getBasePath()));
    listing.assertEquivalent(listStatus);

    // now check without using
    List<LocatedFileStatus> statusThroughNext = toListThroughNextCallsAlone(
        fs.listLocatedStatus(tree.getBasePath())
    );
    TreeScanResults resultsThroughNext = new TreeScanResults(statusThroughNext);
    listStatus.assertFieldsEquivalent("files", listing,
        listStatus.getFiles(),
        resultsThroughNext.getFiles());
  }

  /**
   * Test {@link FileSystem#listFiles(Path, boolean)} on a complex
   * directory tree and the recursive flag set to false.
   * @param tree directory tree to list.
   * @throws Throwable
   */
  protected void checkListFilesComplexDirNonRecursive(TreeScanResults tree)
      throws Throwable {
    describe("Expect non-recursive listFiles(false) to list all entries" +
        " in top dir only");
    FileSystem fs = getFileSystem();
    TreeScanResults listing = new TreeScanResults(
        fs.listFiles(tree.getBasePath(), false));
    listing.assertSizeEquals("listFiles(false)", TREE_FILES, 0, 0);
    verifyFileStats(fs.listFiles(tree.getBasePath(), false));

    // the files listed should match the set of files in a listStatus() call.
    // the directories are not checked
    TreeScanResults listStatus = new TreeScanResults(
        fs.listStatus(tree.getBasePath()));
    listStatus.assertFieldsEquivalent("files", listing,
        listStatus.getFiles(),
        listing.getFiles());
    List<LocatedFileStatus> statusThroughNext = toListThroughNextCallsAlone(
        fs.listFiles(tree.getBasePath(), false));
    TreeScanResults resultsThroughNext = new TreeScanResults(statusThroughNext);
    listStatus.assertFieldsEquivalent("files", listing,
        listStatus.getFiles(),
        resultsThroughNext.getFiles());
  }

  /**
   * Test {@link FileSystem#listFiles(Path, boolean)} on a complex
   * directory tree and the recursive flag set to true.
   * @param tree directory tree to list.
   * @throws Throwable
   */
  protected void checkListFilesComplexDirRecursive(TreeScanResults tree)
      throws Throwable {
    describe("Expect recursive listFiles(true) to" +
        " list all files down the tree");
    FileSystem fs = getFileSystem();
    TreeScanResults listing = new TreeScanResults(
        fs.listFiles(tree.getBasePath(), true));
    // files are checked, but not the directories.
    tree.assertFieldsEquivalent("files", listing, tree.getFiles(),
        listing.getFiles());
    int count = verifyFileStats(fs.listFiles(tree.getBasePath(), true));
    // assert that the content matches that of a tree walk
    describe("verifying consistency with treewalk's files");
    TreeScanResults treeWalk = treeWalk(fs, tree.getBasePath());
    treeWalk.assertFieldsEquivalent("files", listing,
        treeWalk.getFiles(),
        listing.getFiles());
    assertEquals("Size of status list through next() calls",
        count,
        toListThroughNextCallsAlone(
            fs.listFiles(tree.getBasePath(), true)).size());
  }

  @Test
  public void testListFilesNoDir() throws Throwable {
    describe("test the listFiles calls on a path which is not present");
    Path path = path("missing");
    try {
      RemoteIterator<LocatedFileStatus> iterator
          = getFileSystem().listFiles(path, false);
      fail("Expected an exception, got an iterator: " + iterator);
    } catch (FileNotFoundException expected) {
      // expected
    }
    try {
      RemoteIterator<LocatedFileStatus> iterator
          = getFileSystem().listFiles(path, true);
      fail("Expected an exception, got an iterator: " + iterator);
    } catch (FileNotFoundException expected) {
      // expected
    }
  }

  @Test
  public void testListStatusIteratorNoDir() throws Throwable {
    describe("test the listStatusIterator call on a path which is not " +
        "present");
    intercept(FileNotFoundException.class,
        () -> getFileSystem().listStatusIterator(path("missing")));
  }

  @Test
  public void testLocatedStatusNoDir() throws Throwable {
    describe("test the LocatedStatus call on a path which is not present");
    intercept(FileNotFoundException.class,
        () -> getFileSystem().listLocatedStatus(path("missing")));
  }

  @Test
  public void testListStatusNoDir() throws Throwable {
    describe("test the listStatus(path) call on a path which is not present");
    intercept(FileNotFoundException.class,
        () -> getFileSystem().listStatus(path("missing")));
  }

  @Test
  public void testListStatusFilteredNoDir() throws Throwable {
    describe("test the listStatus(path, filter) call on a missing path");
    intercept(FileNotFoundException.class,
        () -> getFileSystem().listStatus(path("missing"), ALL_PATHS));
  }

  @Test
  public void testListStatusFilteredFile() throws Throwable {
    describe("test the listStatus(path, filter) on a file");
    Path f = touchf("liststatus");
    assertEquals(0, getFileSystem().listStatus(f, NO_PATHS).length);
  }

  @Test
  public void testListStatusFile() throws Throwable {
    describe("test the listStatus(path) on a file");
    Path f = touchf("liststatusfile");
    verifyStatusArrayMatchesFile(f, getFileSystem().listStatus(f));
  }

  @Test
  public void testListStatusIteratorFile() throws Throwable {
    describe("test the listStatusIterator(path) on a file");
    Path f = touchf("listStItrFile");

    List<FileStatus> statusList = (List<FileStatus>) iteratorToList(
            getFileSystem().listStatusIterator(f));
    validateListingForFile(f, statusList, false);

    List<FileStatus> statusList2 =
            (List<FileStatus>) iteratorToListThroughNextCallsAlone(
                    getFileSystem().listStatusIterator(f));
    validateListingForFile(f, statusList2, true);
  }

  /**
   * Validate listing result for an input path which is file.
   * @param f file.
   * @param statusList list status of a file.
   * @param nextCallAlone whether the listing generated just using
   *                      next() calls.
   */
  private void validateListingForFile(Path f,
                                      List<FileStatus> statusList,
                                      boolean nextCallAlone) {
    String msg = String.format("size of file list returned using %s should " +
            "be 1", nextCallAlone ?
            "next() calls alone" : "hasNext() and next() calls");
    Assertions.assertThat(statusList)
            .describedAs(msg)
            .hasSize(1);
    Assertions.assertThat(statusList.get(0).getPath())
            .describedAs("path returned should match with the input path")
            .isEqualTo(f);
    Assertions.assertThat(statusList.get(0).isFile())
            .describedAs("path returned should be a file")
            .isEqualTo(true);
  }

  @Test
  public void testListFilesFile() throws Throwable {
    describe("test the listStatus(path) on a file");
    Path f = touchf("listfilesfile");
    List<LocatedFileStatus> statusList = toList(
        getFileSystem().listFiles(f, false));
    assertEquals("size of file list returned", 1, statusList.size());
    assertIsNamedFile(f, statusList.get(0));
    List<LocatedFileStatus> statusList2 = toListThroughNextCallsAlone(
        getFileSystem().listFiles(f, false));
    assertEquals("size of file list returned through next() calls",
        1, statusList2.size());
    assertIsNamedFile(f, statusList2.get(0));
  }

  @Test
  public void testListFilesFileRecursive() throws Throwable {
    describe("test the listFiles(path, true) on a file");
    Path f = touchf("listfilesRecursive");
    List<LocatedFileStatus> statusList = toList(
        getFileSystem().listFiles(f, true));
    assertEquals("size of file list returned", 1, statusList.size());
    assertIsNamedFile(f, statusList.get(0));
    List<LocatedFileStatus> statusList2 = toListThroughNextCallsAlone(
        getFileSystem().listFiles(f, true));
    assertEquals("size of file list returned", 1, statusList2.size());
  }

  @Test
  public void testListLocatedStatusFile() throws Throwable {
    describe("test the listLocatedStatus(path) on a file");
    Path f = touchf("listLocatedStatus");
    List<LocatedFileStatus> statusList = toList(
        getFileSystem().listLocatedStatus(f));
    assertEquals("size of file list returned", 1, statusList.size());
    assertIsNamedFile(f, statusList.get(0));
    List<LocatedFileStatus> statusList2 = toListThroughNextCallsAlone(
        getFileSystem().listLocatedStatus(f));
    assertEquals("size of file list returned through next() calls",
        1, statusList2.size());
  }

  /**
   * Verify a returned status array matches a single named file.
   * @param f filename
   * @param status status array
   */
  private void verifyStatusArrayMatchesFile(Path f, FileStatus[] status) {
    assertEquals(1, status.length);
    FileStatus fileStatus = status[0];
    assertIsNamedFile(f, fileStatus);
  }

  /**
   * Verify that a file status refers to a file at the given path.
   * @param f filename
   * @param fileStatus status to validate
   */
  private void assertIsNamedFile(Path f, FileStatus fileStatus) {
    assertEquals("Wrong pathname in " + fileStatus, f, fileStatus.getPath());
    assertTrue("Not a file: " + fileStatus, fileStatus.isFile());
  }

  /**
   * Touch a file with a given name; return the path.
   * @param name name
   * @return the full name
   * @throws IOException IO Problems
   */
  Path touchf(String name) throws IOException {
    Path path = path(name);
    ContractTestUtils.touch(getFileSystem(), path);
    return path;
  }

  /**
   * Clear the test directory and add an empty subfolder.
   * @return the path to the subdirectory
   * @throws IOException
   */
  private Path createDirWithEmptySubFolder() throws IOException {
    // remove the test directory
    FileSystem fs = getFileSystem();
    Path path = getContract().getTestPath();
    fs.delete(path, true);
    // create a - non-qualified - Path for a subdir
    Path subfolder = path.suffix('/' + this.methodName.getMethodName()
        + "-" + UUID.randomUUID());
    mkdirs(subfolder);
    return subfolder;
  }

  /**
   * Create a test tree.
   * @return the details about the created tree. The files and directories
   * are those created under the path, not the base directory created.
   * @throws IOException
   */
  private TreeScanResults createTestTree() throws IOException {
    return createSubdirs(getFileSystem(), path(methodName.getMethodName()),
        TREE_DEPTH, TREE_WIDTH, TREE_FILES, TREE_FILESIZE);
  }

  /**
   * Scan through a filestatus iterator, get the status of every element and
   * verify core attributes. This should identify a situation where the
   * attributes of a file/dir retrieved in a listing operation do not
   * match the values individually retrieved. That is: the metadata returned
   * in a directory listing is different from the explicitly retrieved data.
   *
   * Timestamps are not compared.
   * @param results iterator to scan
   * @return the number of entries in the result set
   * @throws IOException any IO problem
   */
  private int verifyFileStats(RemoteIterator<LocatedFileStatus> results)
      throws IOException {
    describe("verifying file statuses");
    int count = 0;
    while (results.hasNext()) {
      count++;
      LocatedFileStatus next = results.next();
      FileStatus fileStatus = getFileSystem().getFileStatus(next.getPath());
      assertEquals("isDirectory", fileStatus.isDirectory(), next.isDirectory());
      assertEquals("isFile", fileStatus.isFile(), next.isFile());
      assertEquals("getLen", fileStatus.getLen(), next.getLen());
      assertEquals("getOwner", fileStatus.getOwner(), next.getOwner());
    }
    return count;
  }


  @Test
  public void testListStatusFiltering() throws Throwable {
    describe("Call listStatus() against paths and directories with filtering");
    Path file1 = touchf("file-1.txt");
    touchf("file-2.txt");
    Path parent = file1.getParent();
    FileStatus[] result;

    verifyListStatus(0, parent, NO_PATHS);
    verifyListStatus(2, parent, ALL_PATHS);

    MatchesNameFilter file1Filter = new MatchesNameFilter("file-1.txt");
    result = verifyListStatus(1, parent, file1Filter);
    assertEquals(file1, result[0].getPath());

    verifyListStatus(0, file1, NO_PATHS);
    result = verifyListStatus(1, file1, ALL_PATHS);
    assertEquals(file1, result[0].getPath());
    result = verifyListStatus(1, file1, file1Filter);
    assertEquals(file1, result[0].getPath());

    // empty subdirectory
    Path subdir = path("subdir");
    mkdirs(subdir);
    verifyListStatus(0, subdir, NO_PATHS);
    verifyListStatus(0, subdir, ALL_PATHS);
    verifyListStatus(0, subdir, new MatchesNameFilter("subdir"));
  }

  @Test
  public void testListLocatedStatusFiltering() throws Throwable {
    describe("Call listLocatedStatus() with filtering");
    describe("Call listStatus() against paths and directories with filtering");
    Path file1 = touchf("file-1.txt");
    Path file2 = touchf("file-2.txt");
    Path parent = file1.getParent();
    FileSystem fs = getFileSystem();

    touch(fs, file1);
    touch(fs, file2);
    // this is not closed: ignore any IDE warnings.
    ExtendedFilterFS xfs = new ExtendedFilterFS(fs);
    List<LocatedFileStatus> result;

    verifyListStatus(0, parent, NO_PATHS);
    verifyListStatus(2, parent, ALL_PATHS);

    MatchesNameFilter file1Filter = new MatchesNameFilter("file-1.txt");
    result = verifyListLocatedStatus(xfs, 1, parent, file1Filter);
    assertEquals(file1, result.get(0).getPath());

    verifyListLocatedStatus(xfs, 0, file1, NO_PATHS);
    verifyListLocatedStatus(xfs, 1, file1, ALL_PATHS);
    assertEquals(file1, result.get(0).getPath());
    verifyListLocatedStatus(xfs, 1, file1, file1Filter);
    assertEquals(file1, result.get(0).getPath());
    verifyListLocatedStatusNextCalls(xfs, 1, file1, file1Filter);

    // empty subdirectory
    Path subdir = path("subdir");
    mkdirs(subdir);
    verifyListLocatedStatus(xfs, 0, subdir, NO_PATHS);
    verifyListLocatedStatus(xfs, 0, subdir, ALL_PATHS);
    verifyListLocatedStatusNextCalls(xfs, 0, subdir, ALL_PATHS);
    verifyListLocatedStatus(xfs, 0, subdir, new MatchesNameFilter("subdir"));
  }

  /**
   * Execute {@link FileSystem#listStatus(Path, PathFilter)},
   * verify the length of the result, then return the listing.
   * @param expected expected length
   * @param path path to list
   * @param filter filter to apply
   * @return the listing
   * @throws IOException IO Problems
   */
  private FileStatus[] verifyListStatus(int expected,
      Path path,
      PathFilter filter) throws IOException {
    FileStatus[] result = getFileSystem().listStatus(path, filter);
    assertEquals("length of listStatus(" + path + ", " + filter + " ) " +
        Arrays.toString(result),
        expected, result.length);
    return result;
  }

  /**
   * Execute {@link FileSystem#listLocatedStatus(Path, PathFilter)},
   * generate a list from the iterator, verify the length of the list returned
   * and then return it.
   * @param expected expected length
   * @param path path to list
   * @param filter filter to apply
   * @return the listing
   * @throws IOException IO Problems
   */
  private List<LocatedFileStatus> verifyListLocatedStatus(ExtendedFilterFS xfs,
      int expected,
      Path path,
      PathFilter filter) throws IOException {
    RemoteIterator<LocatedFileStatus> it = xfs.listLocatedStatus(path, filter);
    List<LocatedFileStatus> result = toList(it);
    assertEquals("length of listLocatedStatus(" + path + ", " + filter + " )",
        expected, result.size());
    return result;
  }

  /**
   * Execute {@link FileSystem#listLocatedStatus(Path, PathFilter)},
   * generate a list from the iterator, verify the length of the list returned
   * and then return it.
   * Uses {@link ContractTestUtils#toListThroughNextCallsAlone(RemoteIterator)}
   * to stress the iteration process.
   * @param expected expected length
   * @param path path to list
   * @param filter filter to apply
   * @return the listing
   * @throws IOException IO Problems
   */
  private List<LocatedFileStatus> verifyListLocatedStatusNextCalls(
      ExtendedFilterFS xfs,
      int expected,
      Path path,
      PathFilter filter) throws IOException {
    RemoteIterator<LocatedFileStatus> it = xfs.listLocatedStatus(path, filter);
    List<LocatedFileStatus> result = toListThroughNextCallsAlone(it);
    assertEquals("length of listLocatedStatus(" + path + ", " + filter + " )",
        expected, result.size());
    return result;
  }

  private static final PathFilter ALL_PATHS = new AllPathsFilter();
  private static final PathFilter NO_PATHS = new NoPathsFilter();

  /**
   * Accept everything.
   */
  private static final class AllPathsFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return true;
    }
  }

  /**
   * Accept nothing.
   */
  private static final class NoPathsFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return false;
    }
  }

  /**
   * Path filter which only expects paths whose final name element
   * equals the {@code match} field.
   */
  private static final class MatchesNameFilter implements PathFilter {
    private final String match;

    MatchesNameFilter(String match) {
      this.match = match;
    }

    @Override
    public boolean accept(Path path) {
      return match.equals(path.getName());
    }
  }

  /**
   * A filesystem filter which exposes the protected method
   * {@link #listLocatedStatus(Path, PathFilter)}.
   */
  protected static final class ExtendedFilterFS extends FilterFileSystem {
    public ExtendedFilterFS(FileSystem fs) {
      super(fs);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f,
        PathFilter filter)
        throws IOException {
      return super.listLocatedStatus(f, filter);
    }
  }
}
