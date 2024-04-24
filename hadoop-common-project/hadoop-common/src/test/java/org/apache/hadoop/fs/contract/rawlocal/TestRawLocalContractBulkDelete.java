package org.apache.hadoop.fs.contract.rawlocal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractBulkDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestRawLocalContractBulkDelete extends AbstractContractBulkDeleteTest {

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new RawlocalFSContract(conf);
    }

}
