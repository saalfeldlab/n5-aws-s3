package org.janelia.saalfeldlab.n5.s3.mock;

import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;

import org.janelia.saalfeldlab.n5.s3.AmazonS3KeyValueAccess;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;

public class MockUriTest {

	@Test
	public void testS3Uris() throws URISyntaxException {

		// dummy client
		final AmazonS3 s3 = MockS3Factory.getOrCreateS3();
		final String bucket = "zarr-n5-demo";
		String path = "";

		s3.createBucket(bucket);
		final AmazonS3KeyValueAccess kv = new AmazonS3KeyValueAccess(s3, bucket, false);

		assertTrue(kv.uri(path).toString().startsWith("http://localhost:8001/" + bucket + "/" + path));

		path = "foo.zarr";
		assertTrue(kv.uri(path).toString().startsWith("http://localhost:8001/" + bucket + "/" + path));
	}
}
