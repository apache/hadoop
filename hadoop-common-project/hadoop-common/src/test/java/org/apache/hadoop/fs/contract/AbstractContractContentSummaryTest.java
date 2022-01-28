package org.apache.hadoop.fs.contract;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

public abstract class AbstractContractContentSummaryTest extends AbstractFSContractTestBase  {

    @Test
    public void testGetContentSummary() throws Throwable {
        FileSystem fs = getFileSystem();

        Path parent = path("parent");
        Path nested = path(parent + "/a/b/c");
        Path filePath = path(nested + "file.txt");

        fs.mkdirs(parent);
        fs.mkdirs(nested);
        touch(getFileSystem(), filePath);

        ContentSummary summary = fs.getContentSummary(parent);

        Assertions.assertThat(summary.getDirectoryCount())
                .as("Summary " + summary)
                .isEqualTo(4);

        Assertions.assertThat(summary.getFileCount())
                .as("Summary " + summary)
                .isEqualTo(1);
    }

    @Test
    public void testGetContentSummaryIncorrectPath() throws Throwable {
        FileSystem fs = getFileSystem();

        Path parent = path("parent");
        Path nested = path(parent + "/a");

        fs.mkdirs(parent);

        try {
            fs.getContentSummary(nested);
            Assert.fail("Should throw FileNotFoundException");
        }  catch (FileNotFoundException e) {
          // expected
        }
    }
}
