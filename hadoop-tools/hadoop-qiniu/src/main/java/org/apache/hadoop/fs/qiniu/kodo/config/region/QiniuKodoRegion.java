package org.apache.hadoop.fs.qiniu.kodo.config.region;

import com.qiniu.storage.Region;


public class QiniuKodoRegion {
    private final String regionId;
    private final Region region;
    private final String endpoint;

    public QiniuKodoRegion(String regionId, Region region, String endpoint) {
        this.regionId = regionId;
        this.region = region;
        this.endpoint = endpoint;
    }

    /**
     * 默认走公有云的源站下载域名
     */
    public QiniuKodoRegion(String regionId, Region region) {
        this(regionId, region, String.format("kodo-%s.qiniucs.com", regionId));
    }

    /**
     * 获取源站下载域名
     */
    public String getRegionEndpoint() {
        return endpoint;
    }

    public String getRegionId() {
        return regionId;
    }

    public Region getRegion() {
        return region;
    }
}

