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

import org.apache.hadoop.fs.*;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public abstract class AbstractContractBulkDeleteTest extends AbstractFSContractTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractContractBulkDeleteTest.class);

    protected int pageSize;

    protected Path basePath;

    protected FileSystem fs;

    @Before
    public void setUp() throws Exception {
        fs = getFileSystem();
        basePath = path(getClass().getName());
        pageSize = FileUtil.bulkDeletePageSize(getFileSystem(), basePath);
        fs.mkdirs(basePath);
    }

    public Path getBasePath() {
        return basePath;
    }

    /**
     * Validate the page size for bulk delete operation. Different stores can have different
     * implementations for bulk delete operation thus different page size.
     */
    @Test
    public void validatePageSize() throws Exception {
        Assertions.assertThat(pageSize)
                .describedAs("Page size should be 1 by default for all stores")
                .isEqualTo(1);
    }

    @Test
    public void testPathsSizeEqualsPageSizePrecondition() throws Exception {
        List<Path> listOfPaths = createListOfPaths(pageSize, basePath);
        // Bulk delete call should pass with no exception.
        FileUtil.bulkDelete(getFileSystem(), basePath, listOfPaths);
    }

    @Test
    public void testPathsSizeGreaterThanPageSizePrecondition() throws Exception {
        List<Path> listOfPaths = createListOfPaths(pageSize + 1, basePath);
        intercept(IllegalArgumentException.class,
                () -> FileUtil.bulkDelete(getFileSystem(), basePath, listOfPaths));
    }

    @Test
    public void testPathsSizeLessThanPageSizePrecondition() throws Exception {
        List<Path> listOfPaths = createListOfPaths(pageSize - 1, basePath);
        // Bulk delete call should pass with no exception.
        FileUtil.bulkDelete(getFileSystem(), basePath, listOfPaths);
    }

    @Test
    public void testBulkDeleteSuccessful() throws Exception {
        List<Path> listOfPaths = createListOfPaths(pageSize, basePath);
        for (Path path : listOfPaths) {
            touch(fs, path);
        }
        FileStatus[] fileStatuses = fs.listStatus(basePath);
        Assertions.assertThat(fileStatuses)
                .describedAs("File count after create")
                .hasSize(pageSize);
        assertSuccessfulBulkDelete(FileUtil.bulkDelete(getFileSystem(), basePath, listOfPaths));
        FileStatus[] fileStatusesAfterDelete = fs.listStatus(basePath);
        Assertions.assertThat(fileStatusesAfterDelete)
                .describedAs("File statuses should be empty after delete")
                .isEmpty();
    }

    @Test
    public void validatePathCapabilityDeclared() throws Exception {
        Assertions.assertThat(fs.hasPathCapability(basePath, CommonPathCapabilities.BULK_DELETE))
                .describedAs("Path capability BULK_DELETE should be declared")
                .isTrue();
    }

    @Test
    public void testDeletePathsNotUnderBase() throws Exception {
        List<Path> paths = new ArrayList<>();
        Path pathNotUnderBase = path("not-under-base");
        paths.add(pathNotUnderBase);
        // Should fail as path is not under the base path.
        intercept(IllegalArgumentException.class,
                () -> FileUtil.bulkDelete(getFileSystem(), basePath, paths));
    }

    @Test
    public void testDeletePathsNotExists() throws Exception {
        List<Path> paths = new ArrayList<>();
        Path pathNotExists = new Path(basePath, "not-exists");
        paths.add(pathNotExists);
        // bulk delete call doesn't verify if a path exist or not before deleting.
        assertSuccessfulBulkDelete(FileUtil.bulkDelete(getFileSystem(), basePath, paths));
    }

    @Test
    public void testDeletePathsDirectory() throws Exception {
        List<Path> paths = new ArrayList<>();
        Path dirPath = new Path(basePath, "dir");
        fs.mkdirs(dirPath);
        paths.add(dirPath);
        Path filePath = new Path(dirPath, "file");
        touch(fs, filePath);
        paths.add(filePath);
        // Outcome is undefined. But call shouldn't fail. In case of S3 directories will still be present.
        assertSuccessfulBulkDelete(FileUtil.bulkDelete(getFileSystem(), basePath, paths));
    }

    @Test
    public void testDeleteEmptyDirectory() throws Exception {
        List<Path> paths = new ArrayList<>();
        Path emptyDirPath = new Path(basePath, "empty-dir");
        fs.mkdirs(emptyDirPath);
        paths.add(emptyDirPath);
        // Should pass as empty directory.
        assertSuccessfulBulkDelete(FileUtil.bulkDelete(getFileSystem(), basePath, paths));
    }

    @Test
    public void testDeleteEmptyList() throws Exception {
        List<Path> paths = new ArrayList<>();
        // Empty list should pass.
        assertSuccessfulBulkDelete(FileUtil.bulkDelete(getFileSystem(), basePath, paths));
    }

    @Test
    public void testDeleteSamePathsMoreThanOnce() throws Exception {
        List<Path> paths = new ArrayList<>();
        Path path = new Path(basePath, "file");
        touch(fs, path);
        paths.add(path);
        paths.add(path);
        Path another = new Path(basePath, "another-file");
        touch(fs, another);
        paths.add(another);
        assertSuccessfulBulkDelete(FileUtil.bulkDelete(getFileSystem(), basePath, paths));
    }

    /**
     * This test validates that files to be deleted don't have
     * to be direct children of the base path.
     */
    @Test
    public void testDeepDirectoryFilesDelete() throws Exception {
        List<Path> paths = new ArrayList<>();
        Path dir1 = new Path(basePath, "dir1");
        Path dir2 = new Path(dir1, "dir2");
        Path dir3 = new Path(dir2, "dir3");
        fs.mkdirs(dir3);
        Path file1 = new Path(dir3, "file1");
        touch(fs, file1);
        paths.add(file1);
        assertSuccessfulBulkDelete(FileUtil.bulkDelete(getFileSystem(), basePath, paths));
    }


    @Test
    public void testChildPaths() throws Exception {
        List<Path> paths = new ArrayList<>();
        Path dirPath = new Path(basePath, "dir");
        fs.mkdirs(dirPath);
        paths.add(dirPath);
        Path filePath = new Path(dirPath, "file");
        touch(fs, filePath);
        paths.add(filePath);
        // Should pass as both paths are under the base path.
        assertSuccessfulBulkDelete(FileUtil.bulkDelete(getFileSystem(), basePath, paths));
    }


    public static void assertSuccessfulBulkDelete(List<Map.Entry<Path, String>> entries) {
        Assertions.assertThat(entries)
                .describedAs("return entries should be empty after successful delete")
                .isEmpty();
    }

    private List<Path> createListOfPaths(int count, Path basePath) {
        List<Path> paths = new ArrayList<>();
        for (int i=0; i < count; i++) {
            Path path = new Path(basePath, "file-" + i);
            paths.add(path);
        }
        return paths;
    }
}
