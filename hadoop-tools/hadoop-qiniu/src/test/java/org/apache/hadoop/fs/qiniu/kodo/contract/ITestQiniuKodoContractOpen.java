package org.apache.hadoop.fs.qiniu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class ITestQiniuKodoContractOpen extends AbstractContractOpenTest {
    @Override
    protected AbstractFSContract createContract(Configuration configuration) {
        return new QiniuKodoContract(configuration);
    }
}
