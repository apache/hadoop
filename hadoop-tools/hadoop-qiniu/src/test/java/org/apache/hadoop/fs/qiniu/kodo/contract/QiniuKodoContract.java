package org.apache.hadoop.fs.qiniu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

import java.io.IOException;

public class QiniuKodoContract extends AbstractBondedFSContract {
    private final boolean useMock;

    public QiniuKodoContract(Configuration conf) {
        super(conf);
        addConfResource("qiniu-kodo/contract.xml");
        useMock = conf.getBoolean("fs.qiniu.test.useMock", true);
    }

    @Override
    public FileSystem getTestFileSystem() throws IOException {
        FileSystem fs = super.getTestFileSystem();
        fs.delete(getTestPath(), true);
        return fs;
    }

    @Override
    public String getScheme() {
        return useMock ? "mockkodo" : "kodo";
    }
}