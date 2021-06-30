package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCopyFromLocalTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;
import org.junit.Test;

import java.io.File;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestLocalFSCopyFromLocal extends AbstractContractCopyFromLocalTest {
    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new LocalFSContract(conf);
    }

    @Test
    public void testSourceIsDirectoryAndDestinationIsFile() throws Throwable {
        describe("Source is a directory and destination is a file should fail");

        File file = createTempFile("local");
        File source = createTempDirectory("srcDir");
        Path destination = copyFromLocal(file, false);
        Path sourcePath = new Path(source.toURI());

        intercept(FileAlreadyExistsException.class,
                () -> getFileSystem().copyFromLocalFile(false, true,
                        sourcePath, destination));
    }
}
