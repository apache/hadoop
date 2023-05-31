package org.apache.hadoop.fs.qiniu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;

public class ITestQiniuKodoContractDistCp extends AbstractContractDistCpTest {

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new QiniuKodoContract(conf);
    }

}
