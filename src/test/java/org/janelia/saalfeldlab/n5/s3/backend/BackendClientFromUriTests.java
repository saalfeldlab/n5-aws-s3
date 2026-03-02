package org.janelia.saalfeldlab.n5.s3.backend;

import static org.junit.Assert.assertTrue;

import org.janelia.saalfeldlab.n5.s3.AmazonS3Utils;
import org.junit.Ignore;
import org.junit.Test;

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

	private static void check(String uri) {

		final S3Client s3 = AmazonS3Utils.createS3(uri);
		final String bucket = AmazonS3Utils.getS3Bucket(uri);
		assertTrue("Could not find " + bucket, AmazonS3Utils.bucketExists(s3, bucket));
	}

}
