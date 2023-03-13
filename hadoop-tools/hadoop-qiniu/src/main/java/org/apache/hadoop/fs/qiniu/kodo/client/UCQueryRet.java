package org.apache.hadoop.fs.qiniu.kodo.client;

class UCQueryRet {
    ServerRets[] hosts;

    private static class ServerRets {
        ServerRet io_src;
    }

    private static class ServerRet {
        HostInfoRet src;
    }

    private static class HostInfoRet {
        String[] main;
    }

    public String getIoSrcHost() {
        return hosts[0].io_src.src.main[0];
    }
}
