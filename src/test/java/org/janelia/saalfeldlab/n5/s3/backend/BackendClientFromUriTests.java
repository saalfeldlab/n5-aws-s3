package org.janelia.saalfeldlab.n5.s3.backend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.saalfeldlab.n5.s3.AmazonS3Utils;
import org.junit.Ignore;
import org.junit.Test;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class BackendClientFromUriTests {

	@Test
	public void testS3Uris() {

		check("s3://janelia-cosem-datasets/jrc_mus-choroid-plexus-3/jrc_mus-choroid-plexus-3.zarr");
		check("s3://demo-n5-zarr/boats.zarr/");
	}

	@Test
//	@Ignore("These fail right now")
	public void testVirtualHostStyle() {

		check("https://demo-n5-zarr.s3.us-east-1.amazonaws.com/boats.zarr/");
	}

	@Test
	public void testThirdParty() {

		check("https://uk1s3.embassy.ebi.ac.uk/idr/share/ome2024-ngff-challenge/idr0054/Tonsil%201.zarr");
	}

    final private Map<String, String> regionQueryTests = new LinkedHashMap<>();
    {
        regionQueryTests.put("s3://janelia-cosem-datasets/jrc_mus-choroid-plexus-3/jrc_mus-choroid-plexus-3.zarr", "us-east-1");
        regionQueryTests.put("s3://demo-n5-zarr/boats.zarr/", "us-east-1");
        regionQueryTests.put("s3://sentinel-cogs/sentinel-s2-l2a-cogs", "us-west-2");
        regionQueryTests.put("s3://copernicus-dem-30m/tileList.txt", "eu-central-1");
        regionQueryTests.put("https://s3.amazonaws.com/sentinel-cogs/sentinel-s2-l2a-cogs", "us-west-2");
        regionQueryTests.put("https://sentinel-cogs.s3.amazonaws.com/sentinel-s2-l2a-cogs", "us-west-2");
        regionQueryTests.put("https://copernicus-dem-30m.s3.amazonaws.com/tileList.txt", "eu-central-1");
    }

	@Test
	public void testQueryRegionFromBucket() {

        for (Map.Entry<String, String> entry : regionQueryTests.entrySet()) {
            String uri = entry.getKey();
            String region = entry.getValue();

            Region actual = AmazonS3Utils.queryRegionFromBucket(AmazonS3Utils.UTIL.parseUri(URI.create(uri)));
            Region expected = Region.of(region);
            assertEquals(uri, expected, actual);
        }

		regionQueryTests.forEach((uri, region) -> assertEquals(
				uri,
				Region.of(region),
				AmazonS3Utils.queryRegionFromBucket(AmazonS3Utils.UTIL.parseUri(URI.create(uri)))));
	}

	private static void check(String uri) {

		final S3Client s3 = AmazonS3Utils.createS3(uri);
		final String bucket = AmazonS3Utils.getS3Bucket(uri);
		assertTrue("Could not find " + bucket, AmazonS3Utils.bucketExists(s3, bucket));
	}

}
