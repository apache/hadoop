package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Test;

import java.io.IOException;
import static org.junit.Assert.assertTrue;

public class TestDFSClientWrite {
    @Test
    public void testNoLocalWrite() throws IOException {

        Configuration mockConf =new Configuration();
        mockConf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_NO_LOCAL_WRITE, true);

        DfsClientConf mockDfsClientConf = new DfsClientConf(mockConf);

        assertTrue(mockDfsClientConf.getNoLocalWrite());
    }
}
