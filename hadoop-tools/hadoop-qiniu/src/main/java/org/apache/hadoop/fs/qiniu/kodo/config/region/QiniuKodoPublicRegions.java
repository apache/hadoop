package org.apache.hadoop.fs.qiniu.kodo.config.region;

import com.qiniu.storage.Region;

import java.util.HashMap;
import java.util.Map;

/**
 * 公有云的region配置
 */
public class QiniuKodoPublicRegions {

    private static final Map<String, QiniuKodoRegion> regionMap = new HashMap<>();

    private static void addRegion(String regionId, Region region) {
        addRegion(regionId, region, regionId);
    }

    private static void addRegion(String id, Region region, String regionId) {
        regionMap.put(id, new QiniuKodoRegion(regionId, region));
    }

    static {
        addRegion("cn-east-1", Region.huadong());
        addRegion("z0", Region.huadong(), "cn-east-1");
        addRegion("cn-east-2", Region.huadongZheJiang2());
        addRegion("cn-north-1", Region.huabei());
        addRegion("z1", Region.huabei(), "cn-north-1");
        addRegion("cn-south-1", Region.huanan());
        addRegion("z2", Region.huanan(), "cn-south-1");
        addRegion("us-north-1", Region.beimei());
        addRegion("na0", Region.beimei(), "us-north-1");
        addRegion("ap-southeast-1", Region.xinjiapo());
        addRegion("ap-northeast-1", Region.regionApNorthEast1());
    }

    public static QiniuKodoRegion getRegionById(String regionId) {
        return regionMap.get(regionId);
    }
}
