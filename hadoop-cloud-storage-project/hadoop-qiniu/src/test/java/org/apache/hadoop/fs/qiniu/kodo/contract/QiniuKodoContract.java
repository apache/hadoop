package org.apache.hadoop.fs.qiniu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;
import org.apache.hadoop.fs.qiniu.kodo.Constants;
import org.apache.hadoop.fs.qiniu.kodo.TestConstants;

import java.io.IOException;

public class QiniuKodoContract extends AbstractBondedFSContract {
    private final boolean useMock;

    public QiniuKodoContract(Configuration conf) {
        super(conf);
        addConfResource(TestConstants.FILE_CONTRACT_XML);
        useMock = conf.getBoolean(
                TestConstants.CONFIG_TEST_USE_MOCK_KEY,
                TestConstants.CONFIG_TEST_USE_MOCK_DEFAULT_VALUE
        );
    }

    @Override
    public FileSystem getTestFileSystem() throws IOException {
        FileSystem fs = super.getTestFileSystem();
        fs.delete(getTestPath(), true);
        return fs;
    }

    @Override
    public String getScheme() {
        return useMock ? TestConstants.MOCKKODO_SCHEME : Constants.KODO_SCHEME;
    }
}