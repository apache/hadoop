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

package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractBulkDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.createFiles;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.STORE_IO_RATE_LIMITED_DURATION;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestS3AContractBulkDelete extends AbstractContractBulkDeleteTest {

    private static final Logger LOG = LoggerFactory.getLogger(ITestS3AContractBulkDelete.class);

    /**
     * Delete Page size: {@value}.
     * This is the default page size for bulk delete operation for this contract test.
     * All the tests in this class should pass number of paths equal to or less than
     * this page size during the bulk delete operation.
     */
    private static final int DELETE_PAGE_SIZE = 20;

    @Override
    protected Configuration createConfiguration() {
        Configuration conf = super.createConfiguration();
        S3ATestUtils.disableFilesystemCaching(conf);
        S3ATestUtils.removeBaseAndBucketOverrides(conf,
                Constants.BULK_DELETE_PAGE_SIZE);
        conf.setInt(Constants.BULK_DELETE_PAGE_SIZE, DELETE_PAGE_SIZE);
        return conf;
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new S3AContract(createConfiguration());
    }

    @Override
    public void validatePageSize() throws Exception {
        Assertions.assertThat(pageSize)
                .describedAs("Page size should match the configured page size")
                .isEqualTo(DELETE_PAGE_SIZE);
    }

    @Test
    public void testBulkDeleteZeroPageSizePrecondition() throws Exception {
        Configuration conf = getContract().getConf();
        conf.setInt(Constants.BULK_DELETE_PAGE_SIZE, 0);
        Path testPath = path(getMethodName());
        try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
            intercept(IllegalArgumentException.class,
                    () -> fs.createBulkDelete(testPath));
        }
    }

    @Test
    public void testPageSizeWhenMultiObjectsDisabled() throws Exception {
        Configuration conf = getContract().getConf();
        conf.setBoolean(Constants.ENABLE_MULTI_DELETE, false);
        Path testPath = path(getMethodName());
        try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
            BulkDelete bulkDelete = fs.createBulkDelete(testPath);
            Assertions.assertThat(bulkDelete.pageSize())
                    .describedAs("Page size should be 1 when multi-object delete is disabled")
                    .isEqualTo(1);
        }
    }

    @Test
    public void testRateLimiting() throws Exception {
        Configuration conf = getContract().getConf();
        conf.setInt(Constants.S3A_IO_RATE_LIMIT, 5);
        Path basePath = path(getMethodName());
        try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
            createFiles(fs, basePath, 1, 20, 0);
            FileStatus[] fileStatuses = fs.listStatus(basePath);
            List<Path> paths = Arrays.stream(fileStatuses)
                    .map(FileStatus::getPath)
                    .collect(toList());
            BulkDelete bulkDelete = fs.createBulkDelete(basePath);
            bulkDelete.bulkDelete(paths);
            String mean = STORE_IO_RATE_LIMITED_DURATION + ".mean";
            Assertions.assertThat(fs.getIOStatistics().meanStatistics().get(mean).mean())
                    .describedAs("Rate limiting should not have happened during first delete call")
                    .isEqualTo(0.0);
            bulkDelete.bulkDelete(paths);
            bulkDelete.bulkDelete(paths);
            bulkDelete.bulkDelete(paths);
            Assertions.assertThat(fs.getIOStatistics().meanStatistics().get(mean).mean())
                    .describedAs("Rate limiting should have happened during multiple delete calls")
                    .isGreaterThan(0.0);
        }
    }
}
