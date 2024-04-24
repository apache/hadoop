package org.apache.hadoop.fs.contract.hdfs;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractBulkDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestHDFSContractBulkDelete extends AbstractContractBulkDeleteTest {

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new HDFSContract(conf);
    }

    @BeforeClass
    public static void createCluster() throws IOException {
        HDFSContract.createCluster();
    }

    @AfterClass
    public static void teardownCluster() throws IOException {
        HDFSContract.destroyCluster();
    }
}
