package org.apache.hadoop.fs.qinu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

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
    public String getScheme() {
        return "qiniu";
    }

    public synchronized static Configuration getConfiguration() {
        Configuration cfg = new Configuration();
        cfg.addResource(CONTRACT_XML);
        return cfg;
    }
}