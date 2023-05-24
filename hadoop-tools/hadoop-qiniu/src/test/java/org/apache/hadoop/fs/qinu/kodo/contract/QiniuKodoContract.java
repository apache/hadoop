package org.apache.hadoop.fs.qinu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;
import org.apache.hadoop.fs.qinu.kodo.MockQiniuKodoFileSystem;

import java.io.IOException;
import java.net.URI;

public class QiniuKodoContract extends AbstractBondedFSContract {
    private static final String CONTRACT_XML = "qiniu-kodo/contract.xml";

    /**
     * Constructor: loads the authentication keys if found
     *
     * @param conf configuration to work with
     */
    public QiniuKodoContract(Configuration conf) {
        super(conf);
        addConfResource(CONTRACT_XML);
    }

    @Override
    public FileSystem getTestFileSystem() throws IOException {
        FileSystem fs = super.getTestFileSystem();
        fs.delete(getTestPath(), true);
        return fs;
    }

    @Override
    public String getScheme() {
        return "mockkodo";
    }
}