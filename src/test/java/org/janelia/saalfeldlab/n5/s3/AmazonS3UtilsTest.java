package org.janelia.saalfeldlab.n5.s3;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

public class AmazonS3UtilsTest {

	@Test
	public void testUriParsing() throws URISyntaxException {

		// dummy client
		String[] prefixes = new String[]{
				"s3://",
				"https://s3-eu-west-1.amazonaws.com/",
				"http://localhost:8001/",
		};

		String[] buckets = new String[]{
				"zarr-n5-demo",
				"static.wk.org"};

		String[] paths = new String[]{
				"",
				"foo.zarr",
				"data/sample"};

		for (String prefix : prefixes)
			for (String bucket : buckets)
				for (String path : paths) {
					URI uri = new URI(prefix + bucket + "/" + path);
					assertEquals("bucket from uri", bucket, AmazonS3Utils.getS3Bucket(uri));
					assertEquals("key from uri", path, AmazonS3Utils.getS3Key(uri));
				}

	}

}
