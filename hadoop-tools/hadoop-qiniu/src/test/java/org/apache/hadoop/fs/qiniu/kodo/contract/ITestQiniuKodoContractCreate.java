package org.apache.hadoop.fs.qiniu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class ITestQiniuKodoContractCreate extends AbstractContractCreateTest {
    @Override
    protected AbstractFSContract createContract(Configuration configuration) {
        return new QiniuKodoContract(configuration);
    }
}
