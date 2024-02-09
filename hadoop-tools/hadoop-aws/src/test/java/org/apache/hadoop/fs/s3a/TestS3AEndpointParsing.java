package org.apache.hadoop.fs.s3a;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

public class TestS3AEndpointParsing extends AbstractS3AMockTest {

    private static final String VPC_ENDPOINT = "vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com";
    private static final String NON_VPC_ENDPOINT = "s3.eu-west-1.amazonaws.com";
    private static final String US_WEST_2 = "us-west-2";
    private static final String EU_WEST_1 = "eu-west-1";

    @Test
    public void testVPCEndpoint() {
        Region region = DefaultS3ClientFactory.getS3RegionFromEndpoint(VPC_ENDPOINT, false);
        Assertions.assertThat(region).isEqualTo(Region.of(US_WEST_2));
    }

    @Test
    public void testNonVPCEndpoint() {
        Region region = DefaultS3ClientFactory.getS3RegionFromEndpoint(NON_VPC_ENDPOINT, false);
        Assertions.assertThat(region).isEqualTo(Region.of(EU_WEST_1));
    }
}
