package org.apache.hadoop.fs.qiniu.kodo.client;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;


public class UCQuery {
    private static class UCQueryRet {
        ServerRets[] hosts;

        private static class ServerRets {
            ServerRet io_src;
        }

        private static class ServerRet {
            String[] domains;
        }

        public String getIoSrcHost() {
            return hosts[0].io_src.domains[0];
        }
    }

    public static String getDownloadDomainFromUC(Client client, String accessKey, String bucket) throws QiniuException {
        String ucQueryUrl = String.format(
                "https://uc.qbox.me/v4/query?ak=%s&bucket=%s",
                accessKey,
                bucket
        );
        try {
            return client.get(ucQueryUrl).jsonToObject(UCQueryRet.class).getIoSrcHost();
        } catch (Exception e) {
            throw new QiniuException(e, "get download domain from uc failed");
        }
    }
}
