package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCopyFromLocalTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;

public class TestLocalFSCopyFromLocal extends AbstractContractCopyFromLocalTest {
    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new LocalFSContract(conf);
    }
}
