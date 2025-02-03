package org.janelia.saalfeldlab.n5.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;

public class AmazonS3UtilsTest {

	@Test
	public void testUriParsing() throws URISyntaxException {

		// dummy client
		final String[] prefixes = new String[]{
				"s3://",
				"https://s3-eu-west-1.amazonaws.com/",
				"http://localhost:8001/",
		};

		final String[] buckets = new String[]{
				"zarr-n5-demo",
				"static.wk.org"};

		final String[] paths = new String[]{
				"",
				"foo.zarr",
				"data/sample"};

		for (final String prefix : prefixes)
			for (final String bucket : buckets)
				for (final String path : paths) {
					final String uriString = prefix + bucket + "/" + path;
					final URI uri = new URI(uriString);
					assertEquals("bucket from uri", bucket, AmazonS3Utils.getS3Bucket(uri));
					assertEquals("key from uri", path, AmazonS3Utils.getS3Key(uri));
					AmazonS3Utils.createS3(uriString,
							null,
							StaticCredentialsProvider.create(new AnonymousAWSCredentials()),
							null);
				}

	}

	@Test
	public void getS3InfoTest() {

		final String bucketName = "my-s3-bucket";
		final String[] noKeyTests = new String[]{
				"https://test.com/" + bucketName,
				"http://test.com/" + bucketName + "/",
				"s3://" + bucketName,
				"s3://" + bucketName + "/",

		};


		final String[] onePartKeyWithSlashTests = new String[]{
				"https://test.com/" + bucketName + "/a/",
				"s3://" + bucketName + "/a/",
		};

		final String[] multiPartKeyWithSlashTests = new String[]{
				"https://test.com/" + bucketName + "/a/b/c/d/",
				"s3://" + bucketName + "/a/b/c/d/",
		};



		final String[] onePartKeyNoSlashTests = new String[]{
				"https://test.com/" + bucketName + "/a",
				"s3://" + bucketName + "/a",
		};

		final String[] multiPartKeyNoSlashTests = new String[]{
				"https://test.com/" + bucketName + "/a/b/c/d",
				"s3://" + bucketName + "/a/b/c/d",
		};

		final HashMap<String, String[]> keyToTests = new HashMap<>();

		keyToTests.put("", noKeyTests);
		keyToTests.put("a/", onePartKeyWithSlashTests);
		keyToTests.put("a/b/c/d/", multiPartKeyWithSlashTests);
		keyToTests.put("a", onePartKeyNoSlashTests);
		keyToTests.put("a/b/c/d", multiPartKeyNoSlashTests);

		for (final Map.Entry<String, String[]> tests : keyToTests.entrySet()) {
			final String expectedKey = tests.getKey();
			for (final String uri : tests.getValue()) {
				assertEquals(bucketName, AmazonS3Utils.getS3Bucket(uri));
				assertEquals("Unexpected key for " + uri, expectedKey, AmazonS3Utils.getS3Key(uri));
			}
		}

		assertNull("Invalid URI should return null for bucket", AmazonS3Utils.getS3Bucket("invalid uri \\ _ ~ 435:  q2234[;5."));
		assertEquals("Invalid URI returns empty string for key", "", AmazonS3Utils.getS3Key("invalid uri \\ _ ~ 435:  q2234[;5."));
	}

}
