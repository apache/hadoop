package org.apache.hadoop.fs.contract.localfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractContentSummaryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestLocalFSContractContentSummary extends AbstractContractContentSummaryTest {

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new LocalFSContract(conf);
    }
}
