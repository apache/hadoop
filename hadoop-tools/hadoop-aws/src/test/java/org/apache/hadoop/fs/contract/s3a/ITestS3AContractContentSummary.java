package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractContentSummaryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;


public class ITestS3AContractContentSummary extends AbstractContractContentSummaryTest {

    @Test
    public void testGetContentSummaryDir() throws Throwable {
        describe("getContentSummary on test dir with children");
        S3AFileSystem fs = getFileSystem();
        Path baseDir = methodPath();

        // Nested folders created separately will return as separate objects in listFiles()
        fs.mkdirs(new Path(baseDir + "/a"));
        fs.mkdirs(new Path(baseDir + "/a/b"));
        fs.mkdirs(new Path(baseDir + "/a/b/a"));

        // Will return as one object
        fs.mkdirs(new Path(baseDir + "/d/e/f"));

        Path filePath = new Path(baseDir, "a/b/file");
        touch(fs, filePath);

        // look at path to see if it is a file
        // it is not: so LIST
        final ContentSummary summary = fs.getContentSummary(baseDir);

        Assertions.assertThat(summary.getDirectoryCount())
                .as("Summary " + summary)
                .isEqualTo(7);
        Assertions.assertThat(summary.getFileCount())
                .as("Summary " + summary)
                .isEqualTo(1);
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new S3AContract(conf);
    }

    @Override
    public S3AFileSystem getFileSystem() {
        return (S3AFileSystem) super.getFileSystem();
    }

}
