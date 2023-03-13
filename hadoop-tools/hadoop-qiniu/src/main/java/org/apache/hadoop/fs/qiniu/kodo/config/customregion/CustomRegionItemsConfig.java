package org.apache.hadoop.fs.qiniu.kodo.config.customregion;

import com.qiniu.storage.Region;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;
import org.apache.hadoop.fs.qiniu.kodo.config.MissingConfigFieldException;

public class CustomRegionItemsConfig extends AConfigBase {

    public CustomRegionItemsConfig(Configuration conf, String namespace) {
        super(conf, namespace);
    }

    private String getCustomRegionFieldString(String customId, String field) throws MissingConfigFieldException {
        String key = String.format("%s.%s.%s", namespace, customId, field);
        String value = conf.get(key);
        if (value != null) return value;
        throw new MissingConfigFieldException(key);
    }

    public Region buildCustomSdkRegion(String customId) throws MissingConfigFieldException {
        // 资源管理，资源列表，资源处理类域名
        String rsHost = getCustomRegionFieldString(customId, "rsHost");
        String rsfHost = getCustomRegionFieldString(customId, "rsfHost");
        String apiHost = getCustomRegionFieldString(customId, "apiHost");

        // 源站上传，加速上传，源站下载
        String[] srcUpHosts = conf.getStrings(String.format("%s.%s.srcUpHosts", namespace, customId), new String[0]);
        String[] accUpHosts = conf.getStrings(String.format("%s.%s.accUpHosts", namespace, customId), new String[0]);
        String iovipHost = getCustomRegionFieldString(customId, "iovipHost");

        return new Region.Builder()
                .apiHost(apiHost)
                .rsfHost(rsfHost)
                .rsHost(rsHost)
                .iovipHost(iovipHost)
                .srcUpHost(srcUpHosts)
                .accUpHost(accUpHosts)
                .build();
    }

}
