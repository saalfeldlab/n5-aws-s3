package org.janelia.saalfeldlab.n5.s3;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.janelia.saalfeldlab.n5.N5Exception;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Request;

public class AmazonS3Utils {
	public static final Pattern AWS_ENDPOINT_PATTERN = Pattern.compile("^(.+\\.)?(s3\\..*amazonaws\\.com)", Pattern.CASE_INSENSITIVE);
	public final static Pattern S3_SCHEME = Pattern.compile("s3", Pattern.CASE_INSENSITIVE);

	private AmazonS3Utils() {

	}

	public static String getS3Bucket(final String uri) {

		try {
			return getS3Bucket(new URI(uri));
		} catch (final URISyntaxException e) {
		}
		return null;
	}

	public static String getS3Bucket(final URI uri) {

		try {
			return new AmazonS3URI(uri).getBucket();
		} catch (final IllegalArgumentException e) {
		}
		// parse bucket manually when AmazonS3URI can't
		final String path = uri.getPath().replaceFirst("^/", "");
		return path.split("/")[0];
	}

	public static String getS3Key(final String uri) {

		try {
			return getS3Key(new URI(uri));
		} catch (final URISyntaxException e) {
		}
		return "";
	}

	public static String getS3Key(final URI uri) {

		try {
			// if key is null, return the empty string
			final String key = new AmazonS3URI(uri).getKey();
			return key == null ? "" : key;
		} catch (final IllegalArgumentException e) {
		}
		// parse key manually when AmazonS3URI can't
		final StringBuilder keyBuilder = new StringBuilder();
		final String[] parts = uri.getPath().replaceFirst("^/", "").split("/");
		for (int i = 1; i < parts.length; i++) {
			keyBuilder.append(parts[i]);
			if (i != parts.length - 1 || uri.getPath().endsWith("/"))
					keyBuilder.append("/");
		}
		return keyBuilder.toString();
	}

	public static boolean areAnonymous(final AWSCredentialsProvider credsProvider) {

		final AWSCredentials creds = credsProvider.getCredentials();
		// AnonymousAWSCredentials do not have an equals method
		if (creds instanceof AnonymousAWSCredentials)
			return true;

		return creds.getAWSAccessKeyId() == null && creds.getAWSSecretKey() == null;
	}

	public static Regions getS3Region(final AmazonS3URI uri, @Nullable final String region) {

		final Regions regionFromUri = parseRegion(uri.getRegion());
		return regionFromUri != null ? regionFromUri : parseRegion(region);
	}

	private static Regions parseRegion(String stringRegionFromUri) {

		return stringRegionFromUri != null ? Regions.fromName(stringRegionFromUri) : null;
	}

	public static AWSCredentialsProvider getS3Credentials(final AWSCredentials s3Credentials, final boolean s3Anonymous) {

		if (s3Credentials != null) {
			return new AWSStaticCredentialsProvider(s3Credentials);
		} else {
			// if not anonymous, try finding credentials
			if (!s3Anonymous)
				return new DefaultAWSCredentialsProviderChain();
			else
				return null;
		}
	}

	public static AmazonS3 createS3(final String uri) {

		return createS3(uri, (String)null, null, null);
	}

	public static AmazonS3 createS3(final String uri, @Nullable final String s3Endpoint, @Nullable final AWSCredentialsProvider s3Credentials, @Nullable String region) {

		try {
			final AmazonS3URI s3Uri = new AmazonS3URI(uri);
			return createS3(s3Uri, s3Endpoint, s3Credentials, region);
		} catch (final IllegalArgumentException e) {
			// if AmazonS3URI does not like the form of the uri
			try {
				final URI asURI = new URI(uri);
				final URI endpointUri = new URI(asURI.getScheme(), asURI.getAuthority(), null, null, null);
				return createS3(AmazonS3Utils.getS3Bucket(uri), s3Credentials, new AwsClientBuilder.EndpointConfiguration(endpointUri.toString(), null), null);
			} catch (final URISyntaxException e1) {
				throw new N5Exception("Could not create s3 client from uri: " + uri, e1);
			}
		}
	}

	public static AmazonS3 createS3(final AmazonS3URI s3Uri, @Nullable final String s3Endpoint, @Nullable final AWSCredentialsProvider s3Credentials, @Nullable final String region) {

		AwsClientBuilder.EndpointConfiguration endpointConfiguration = null;
		if (!S3_SCHEME.matcher(s3Uri.getURI().getScheme()).matches()) {
			endpointConfiguration = createEndpointConfiguration(s3Uri, s3Endpoint);
		}
		return createS3(s3Uri.getBucket(), s3Credentials, endpointConfiguration, getS3Region(s3Uri, region));
	}

	public static AwsClientBuilder.EndpointConfiguration createEndpointConfiguration(final AmazonS3URI s3Uri, @Nullable final String s3Endpoint) {

		AwsClientBuilder.EndpointConfiguration endpointConfiguration;
		if (s3Endpoint != null)
			endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(s3Endpoint, null);
		else {
			final Matcher matcher = AmazonS3Utils.AWS_ENDPOINT_PATTERN.matcher(s3Uri.getURI().getHost());
			if (matcher.find())
				endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(matcher.group(2), s3Uri.getRegion());
			else
				endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(s3Uri.getURI().getHost(), s3Uri.getRegion());
		}
		return endpointConfiguration;
	}

	public static AmazonS3 createS3(
			final String bucketName,
			@Nullable final AWSCredentialsProvider credentialsProvider,
			@Nullable final AwsClientBuilder.EndpointConfiguration endpointConfiguration,
			@Nullable final Regions region) {

		final boolean isAmazon = endpointConfiguration == null || AmazonS3Utils.AWS_ENDPOINT_PATTERN.matcher(endpointConfiguration.getServiceEndpoint()).find();
		final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

		if (!isAmazon)
			builder.withPathStyleAccessEnabled(true);

		if (credentialsProvider == null)
			builder.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));
		else
			builder.withCredentials(credentialsProvider);

		if (endpointConfiguration != null)
			builder.withEndpointConfiguration(endpointConfiguration);
		else if (region != null)
			builder.withRegion(region);
		else
			builder.withRegion("us-east-1");

		final AmazonS3 s3 = builder.build();
		// try to listBucket if we are anonymous, if we cannot, don't use anonymous.
		if (credentialsProvider == null) {

			// I initially tried checking whether the bucket exists, but
			// that, apparently, returns even when the client does not have access
			if (!s3.doesBucketExistV2(bucketName) || !canListBucket(s3, bucketName)) {
				// bucket not detected with anonymous credentials, try detecting credentials
				// and return it even if it can't detect the bucket, since there's nothing else to do
				builder.withCredentials(new DefaultAWSCredentialsProviderChain());
				return builder.build();
			}
		}
		return s3;
	}

	private static boolean canListBucket(final AmazonS3 s3, final String bucket) {

		final ListObjectsV2Request request = new ListObjectsV2Request();
		request.setBucketName(bucket);
		request.setMaxKeys(1);

		try {
			// list objects will throw an AmazonS3Exception (Access Denied) if this client does not have access
			s3.listObjectsV2(request);
			return true;
		} catch (final AmazonS3Exception e) {
			return false;
		}
	}
}
