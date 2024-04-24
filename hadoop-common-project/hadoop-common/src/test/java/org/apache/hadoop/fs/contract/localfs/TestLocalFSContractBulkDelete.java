package org.apache.hadoop.fs.contract.localfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractBulkDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestLocalFSContractBulkDelete extends AbstractContractBulkDeleteTest {

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new LocalFSContract(conf);
    }
}
