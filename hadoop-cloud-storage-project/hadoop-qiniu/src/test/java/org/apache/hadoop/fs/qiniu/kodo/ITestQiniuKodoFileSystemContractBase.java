package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ITestQiniuKodoFileSystemContractBase extends FileSystemContractBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ITestQiniuKodoFileSystemContractBase.class);

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(TestConstants.FILE_CORE_SITE_XML);
        conf.addResource(TestConstants.FILE_CONTRACT_TEST_OPTIONS_XML);

        if (conf.getBoolean(
                TestConstants.CONFIG_TEST_USE_MOCK_KEY,
                TestConstants.CONFIG_TEST_USE_MOCK_DEFAULT_VALUE
        )) {
            fs = new MockQiniuKodoFileSystem();
            fs.initialize(URI.create(conf.get(TestConstants.CONFIG_TEST_CONTRACT_MOCK_FS_KEY)), conf);
        } else {
            fs = new QiniuKodoFileSystem();
            fs.initialize(URI.create(conf.get(TestConstants.CONFIG_TEST_CONTRACT_FS_KEY)), conf);
        }

        fs.delete(getTestBaseDir(), true);
    }

    @Override
    protected void rename(Path src, Path dst, boolean renameSucceeded,
                          boolean srcExists, boolean dstExists) throws IOException {
        try {
            assertEquals("Rename result", renameSucceeded, fs.rename(src, dst));
        } catch (FileAlreadyExistsException faee) {
            if (renameSucceeded) {
                fail("Expected rename succeeded but " + faee);
            }
        }
        assertEquals("Source exists", srcExists, fs.exists(src));
        assertEquals("Destination exists" + dst, dstExists, fs.exists(dst));
    }
}
