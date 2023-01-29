package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.Region;

import java.util.HashMap;
import java.util.Map;

public class QiniuKodoRegionManager {
    public static class QiniuKodoRegion {
        private final String regionId;
        private final Region region;

        public QiniuKodoRegion(String regionId, Region region) {
            this.regionId = regionId;
            this.region = region;
        }

        /**
         * 获取源站域名
         */
        public String getRegionEndpoint() {
            return String.format("kodo-%s.qiniucs.com", regionId);
        }

        public String getRegionId() {
            return regionId;
        }

        public Region getRegion() {
            return region;
        }
    }

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

    public static class RegionNotFoundException extends Exception {
        public RegionNotFoundException(String regionId) {
            super(String.format("RegionId: %s not found.", regionId));
        }
    }

    public static QiniuKodoRegion getRegionById(String regionId) throws QiniuException {
        QiniuKodoRegion region = regionMap.get(regionId);
        if (region != null) return region;
        throw new QiniuException(new RegionNotFoundException(regionId));
    }
}
