package org.janelia.saalfeldlab.n5.s3.backend;

import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;

import org.janelia.saalfeldlab.n5.s3.AmazonS3KeyValueAccess;
import org.junit.Test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class BackendUriTest {

	@Test
	public void testS3Uris() throws URISyntaxException {

		final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
				.build();

		final String bucket = "demo-n5-zarr";
		String path = "";

		// s3 style
		final AmazonS3KeyValueAccess kv = new AmazonS3KeyValueAccess(s3, bucket, false);
		assertTrue(kv.uri(path).toString(), check(path, kv, "s3:/", bucket, path));

		path = "foo.zarr";
		assertTrue(kv.uri(path).toString(), check(path, kv, "s3:/", bucket, path));

		path = "amazonaws.com.zarr";
		assertTrue(kv.uri(path).toString(), check(path, kv, "s3:/", bucket, path));


		// ensure we return s3:// uris even when the client uses an amazon client with path style access
		final AmazonS3 s3ep = AmazonS3ClientBuilder.standard()
				.withPathStyleAccessEnabled(true)
				.withEndpointConfiguration(new EndpointConfiguration("s3.amazonaws.com", "us-east-1"))
				.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
				.build();

		final AmazonS3KeyValueAccess kvep = new AmazonS3KeyValueAccess(s3ep, bucket, false);
		assertTrue(kvep.uri(path).toString(), check(path, kvep, "s3:/", bucket, path));

		path = "foo.zarr";
		assertTrue(kvep.uri(path).toString(), check(path, kvep, "s3:/", bucket, path));

		path = "amazonaws.com.zarr";
		assertTrue(kvep.uri(path).toString(), check(path, kvep, "s3:/", bucket, path));
	}

	@Test
	public void testEndpointUris() throws URISyntaxException {

		// embl endpoint
		final String emblEndpoint = "s3.embl.de";
		final String emblBucket = "i2k-2020";
		String path = "";

		final AmazonS3 emblS3 = AmazonS3ClientBuilder.standard()
				.withEndpointConfiguration(new EndpointConfiguration(emblEndpoint, ""))
				.withPathStyleAccessEnabled(true)
				.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
				.build();

		final AmazonS3KeyValueAccess ekv = new AmazonS3KeyValueAccess(emblS3, emblBucket, false);
		assertTrue(ekv.uri(path).toString(), check(path, ekv, "https:/", emblEndpoint, emblBucket, path));

		path = "foo.zarr";
		assertTrue(ekv.uri(path).toString(), check(path, ekv, "https:/", emblEndpoint, emblBucket, path));

		path = "amazonaws.com.zarr";
		assertTrue(ekv.uri(path).toString(), check(path, ekv, "https:/", emblEndpoint, emblBucket, path));


		// idr endpoint
		final String idrEndpoint = "uk1s3.embassy.ebi.ac.uk";
		final String idrBucket = "idr";

		final AmazonS3 idrs3 = AmazonS3ClientBuilder.standard()
				.withEndpointConfiguration(new EndpointConfiguration(idrEndpoint, ""))
				.withPathStyleAccessEnabled(true)
				.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
				.build();

		final AmazonS3KeyValueAccess ikv = new AmazonS3KeyValueAccess(idrs3, idrBucket, false);
		assertTrue(ikv.uri(path).toString(), check(path, ikv, "https:/", idrEndpoint, idrBucket, path));

		path = "foo.zarr";
		assertTrue(ikv.uri(path).toString(), check(path, ikv, "https:/", idrEndpoint, idrBucket, path));

		path = "amazonaws.com.zarr";
		assertTrue(ikv.uri(path).toString(), check(path, ikv, "https:/", idrEndpoint, idrBucket, path));
	}

	private static final boolean check(final String path, final AmazonS3KeyValueAccess kv, final String... components) throws URISyntaxException {

		return kv.uri(path).toString().startsWith(String.join("/", components));
	}
}
