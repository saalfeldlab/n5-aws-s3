package org.janelia.saalfeldlab.n5.s3.backend;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.s3.AmazonS3KeyValueAccess;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Tests;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BackendUriPathTests {

	@Parameterized.Parameters(name = "path: \"{0}\"")
	public static Collection<Object[]> data() throws URISyntaxException {

		return Arrays.asList(new Object[][]{
				{""},
				{"foo.zarr"},
				{"amazonaws.com.zarr"},
		});
	}

	@Parameterized.Parameter()
	public String path;

	private String bucket;
	private AmazonS3 s3;

	@After
	public void removeTestBucket() {

		if (s3 != null && bucket != null && s3.doesBucketExistV2(bucket)) {
			s3.deleteBucket(bucket);
		}
		s3 = null;
		bucket = null;
	}

	/**
	 * @param s3 to store in this instance for @After {@link #removeTestBucket()}
	 * @return the bucket name, which is stored internally for removal during @After {@link #removeTestBucket()}
	 */
	protected String getTempBucket(final AmazonS3 s3) {

		this.s3 = s3;
		this.bucket = N5AmazonS3Tests.tempBucketName();
		return this.bucket;
	}

	@Test
	public void testS3URIs() throws URISyntaxException {

		s3 = AmazonS3ClientBuilder.standard().build();

		final URI s3URI = N5URI.encodeAsUri("s3://" + getTempBucket(s3));
		final AmazonS3KeyValueAccess kvep = new AmazonS3KeyValueAccess(s3, s3URI, true);
		check(kvep, s3URI, path);
	}

	@Test
	public void testS3URIsWithPathStyleAccess() throws URISyntaxException {

		// ensure we return s3:// uris even when the client uses an amazon client with path style access
		s3 = AmazonS3ClientBuilder.standard()
				.withPathStyleAccessEnabled(true)
				.withEndpointConfiguration(new EndpointConfiguration("s3.amazonaws.com", "us-east-1"))
				.build();

		final URI s3URI = N5URI.encodeAsUri("s3://" + getTempBucket(s3));
		final AmazonS3KeyValueAccess kvep = new AmazonS3KeyValueAccess(s3, s3URI, true);
		check(kvep, s3URI, path);
	}

	@Test
	public void testEMBLUriPaths() throws URISyntaxException {

		testPathAtPublicURI(N5URI.encodeAsUri("https://s3.embl.de/i2k-2020"), path);
	}

	@Test
	public void testEMIUriPaths() throws URISyntaxException {

		testPathAtPublicURI(N5URI.encodeAsUri("https://uk1s3.embassy.ebi.ac.uk/idr"), path);
	}

	private static void testPathAtPublicURI(URI uri, String path) throws URISyntaxException {

		final AmazonS3 s3;
		try {
			s3 = AmazonS3ClientBuilder.standard()
					.withEndpointConfiguration(new EndpointConfiguration(uri.getAuthority(), ""))
					.withPathStyleAccessEnabled(true)
					.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
					.build();
		} catch (Exception e) {
			Assume.assumeNoException(e);
			throw e;
		}

		final AmazonS3KeyValueAccess kva = new AmazonS3KeyValueAccess(s3, uri, false);
		check(kva, uri, path);
	}

	private static void check(final AmazonS3KeyValueAccess kv, final URI uri, final String path) throws URISyntaxException {

		final String expected = String.join("/", uri.toString(), path).replaceFirst("/$", "");
		final URI actual = kv.uri(path);
		assertEquals(expected, actual.toString());
	}
}
