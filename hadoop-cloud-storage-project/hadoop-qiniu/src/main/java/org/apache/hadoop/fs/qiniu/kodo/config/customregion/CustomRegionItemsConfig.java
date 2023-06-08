package org.apache.hadoop.fs.qiniu.kodo.config.customregion;

import com.qiniu.storage.Region;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;
import org.apache.hadoop.fs.qiniu.kodo.config.MissingConfigFieldException;

public class CustomRegionItemsConfig extends AConfigBase {

    public CustomRegionItemsConfig(Configuration conf, String namespace) {
        super(conf, namespace);
    }

    private String getCustomRegionStringFieldString(String customId, String field) throws MissingConfigFieldException {
        String key = String.format("%s.%s.%s", namespace, customId, field);
        String value = conf.get(key);
        if (value != null) return value;
        throw new MissingConfigFieldException(key);
    }

    public Region buildCustomSdkRegion(String customId) throws MissingConfigFieldException {
        // uc server address, if set, use autoRegion directly
        String ucServer = conf.get(String.format("%s.%s.%s", namespace, customId, "ucServer"));
        if (ucServer != null) {
            return Region.autoRegion(ucServer);
        }

        // resource management, resource list, resource processing class domain name
        String rsHost = getCustomRegionStringFieldString(customId, "rsHost");
        String rsfHost = getCustomRegionStringFieldString(customId, "rsfHost");
        String apiHost = getCustomRegionStringFieldString(customId, "apiHost");

        // source station upload, accelerated upload, source station download
        String[] srcUpHosts = conf.getStrings(String.format("%s.%s.srcUpHosts", namespace, customId), new String[0]);
        String[] accUpHosts = conf.getStrings(String.format("%s.%s.accUpHosts", namespace, customId), new String[0]);
        String iovipHost = getCustomRegionStringFieldString(customId, "iovipHost");
        String ioSrcHost = getCustomRegionStringFieldString(customId, "ioSrcHost");

        return new Region.Builder()
                .apiHost(apiHost)
                .rsfHost(rsfHost)
                .rsHost(rsHost)
                .iovipHost(iovipHost)
                .ioSrcHost(ioSrcHost)
                .srcUpHost(srcUpHosts)
                .accUpHost(accUpHosts)
                .build();
    }

}
