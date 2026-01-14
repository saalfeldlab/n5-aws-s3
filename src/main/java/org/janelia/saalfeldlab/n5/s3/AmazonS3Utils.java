package org.janelia.saalfeldlab.n5.s3;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5URI;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.CredentialUtils;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.utils.Validate;

public class AmazonS3Utils {

	public static final Pattern AWS_ENDPOINT_PATTERN = Pattern.compile("^(.+\\.)?(s3\\..*amazonaws\\.com)", Pattern.CASE_INSENSITIVE);
	public final static Pattern S3_SCHEME = Pattern.compile("s3", Pattern.CASE_INSENSITIVE);

	// A Region is required, but we won't be making use of it
	public static final S3Utilities UTIL = S3Utilities.builder()
			.region(Region.US_EAST_1).build();

	private AmazonS3Utils() {
	}

    /**
     * Deletes all objects in the specified S3 bucket and then deletes the bucket.
     *
     * @param s3     The S3Client instance to use for the S3 operations.
     * @param bucket The name of the S3 bucket to delete.
     * @throws S3Exception if any error occurs during the S3 operations.
     */
	public static boolean deleteBucket( final S3Client s3, final String bucket ) throws S3Exception {

		// To delete a bucket, all the objects in the bucket must be deleted first.
		ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).build();
		ListObjectsV2Response listObjectsV2Response;

		do {
			listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
			for (S3Object s3Object : listObjectsV2Response.contents()) {
				DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(bucket).key(s3Object.key()).build();
				s3.deleteObject(request);
			}
		} while (listObjectsV2Response.isTruncated());
		final DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
		s3.deleteBucket(deleteBucketRequest);

		return true;
	}

	public static boolean bucketExists(final S3Client s3, final String bucketName) {

		// see
		// https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/javav2/example_code/s3/src/main/java/com/example/s3/DoesBucketExist.java
		try {
			Validate.notEmpty(bucketName, "The bucket name must not be null or an empty string.", "");
			s3.getBucketAcl(r -> r.bucket(bucketName));
			return true;
        } catch (AwsServiceException ase) {
            // A redirect error or an AccessDenied exception means the bucket exists but it's not in this region
            // or we don't have permissions to it.
            if ((ase.statusCode() == HttpStatusCode.MOVED_PERMANENTLY) || "AccessDenied".equals(ase.awsErrorDetails().errorCode())) {
                return true;
            }
            if (ase.statusCode() == HttpStatusCode.NOT_FOUND) {
                return false;
            }
            throw ase;
        }
	}

	public static String getS3Bucket(final String uri) {

		try {
			return getS3Bucket(N5URI.getAsUri(uri));
		} catch (final N5Exception e) {
		}
		return null;
	}

	public static String getS3Bucket(final URI uri) {

		try {
			final Optional<String> bucketOpt = UTIL.parseUri(uri).bucket();
			if (bucketOpt.isPresent())
				return bucketOpt.get();
		} catch (final IllegalArgumentException e) {
		}
		// parse bucket manually when AmazonS3URI can't
		final String path = uri.getPath().replaceFirst("^/", "");
		return path.split("/")[0];
	}

	public static String getS3Key(final String uri) {

		try {
			return getS3Key(N5URI.getAsUri(uri));
		} catch (final N5Exception e) {
		}
		return "";
	}

	public static String getS3Key(final URI uri) {

		try {
			// if key is null, return the empty string
			final Optional<String> keyOpt = UTIL.parseUri(uri).key();
			if( keyOpt.isPresent())
				return keyOpt.get() == null ? "" : keyOpt.get();
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

	public static boolean areAnonymous(final AwsCredentialsProvider credsProvider) {

		if (credsProvider instanceof AnonymousCredentialsProvider)
			return true;

		final AwsCredentials creds = credsProvider.resolveCredentials();
		if (CredentialUtils.isAnonymous(creds))
			return true;
		else
			return false;
	}

	public static Region getS3Region(final S3Uri uri, final String region) {

		return getS3Region(uri, Region.of(region));
	}

	public static Region getS3Region(final S3Uri uri, final Region region) {

		final Region regionFromUri = uri.region().orElse(null);
		return regionFromUri != null ? regionFromUri : region;
	}

	public static AwsCredentialsProvider getS3Credentials(final AwsCredentials s3Credentials, final boolean s3Anonymous) {

		if (s3Credentials != null) {
			final StaticCredentialsProvider provider = StaticCredentialsProvider.create(s3Credentials);
			return provider;
		} else {
			// if not anonymous, try finding credentials
			if (!s3Anonymous) {
				final DefaultCredentialsProvider provider = DefaultCredentialsProvider.create();
				return provider;
			}
			else {
				return null;
			}
		}
	}

	public static S3Client createS3(final String uri) {

		return createS3(uri, (String)null, null, null);
	}

	public static S3Client createS3(final String uri, @Nullable final String s3Endpoint, @Nullable final AwsCredentialsProvider s3Credentials,
			@Nullable String region) {

		return createS3(uri, s3Endpoint, s3Credentials, null, region);
	}

	public static S3Client createS3(final String uri, @Nullable final String s3Endpoint, @Nullable final AwsCredentialsProvider s3Credentials,
			@Nullable final SdkHttpClient.Builder<?> clientBuilder, @Nullable String region) {

		try {
			final S3Uri s3Uri = AmazonS3Utils.UTIL.parseUri(new URI(uri));
			return createS3(s3Uri, s3Endpoint, s3Credentials, region);
		} catch (final IllegalArgumentException | URISyntaxException  e) {
			// if AmazonS3URI does not like the form of the uri
			try {
				final URI asURI = new URI(uri);
				final URI endpointUri = new URI(asURI.getScheme(), asURI.getAuthority(), null, null, null);
				final Endpoint endpoint = Endpoint.builder().url(endpointUri).build();
				return createS3(AmazonS3Utils.getS3Bucket(uri), s3Credentials, endpoint, null);
			} catch (final URISyntaxException e1) {
				throw new N5Exception("Could not create s3 client from uri: " + uri, e1);
			}
		}
	}

	public static S3Client createS3(final S3Uri s3Uri, @Nullable final String s3Endpoint, @Nullable final AwsCredentialsProvider s3Credentials,
			@Nullable final String region) {

		return createS3(s3Uri, s3Endpoint, s3Credentials, null, region);
	}

	public static S3Client createS3(final S3Uri s3Uri, @Nullable final String s3Endpoint, @Nullable final AwsCredentialsProvider s3Credentials,
			@Nullable final SdkHttpClient.Builder<?> clientBuilder, @Nullable final String region) {

		// TODO: this changed a lot - validate me
		final Region defaultRegion = region == null ? Region.US_EAST_1 : Region.of(region);
		final Endpoint endpoint = Endpoint.builder().url(s3Uri.uri()).build();
		final Optional<String> bucketOpt = s3Uri.bucket();
		if (bucketOpt.isPresent())
			return createS3(bucketOpt.get(), s3Credentials, endpoint, clientBuilder, getS3Region(s3Uri, defaultRegion));
		else
			throw new N5Exception("Could not infer bucket name from uri: " + s3Uri);
	}

	public static S3Client createS3(
			final String bucketName,
			@Nullable final AwsCredentialsProvider credentialsProvider,
			@Nullable final Endpoint endpoint,
			@Nullable final Region region) {

		return createS3(bucketName, credentialsProvider, endpoint, null, region);
	}

	public static S3Client createS3(
			final String bucketName,
			@Nullable final AwsCredentialsProvider credentialsProvider,
			@Nullable final Endpoint endpoint,
			@Nullable final SdkHttpClient.Builder<?> clientBuilder,
			@Nullable final Region region) {

		// TODO figure this out 
		final boolean isAmazon = endpoint == null || AmazonS3Utils.AWS_ENDPOINT_PATTERN.matcher(endpoint.url().toString()).find();

		final S3ClientBuilder builder = S3Client.builder();

		// Forcing path style is necessary for at least some non-amazon services
		// (e.g. IDR), as of May 2025
		if (!isAmazon)
			builder.forcePathStyle(true);

		if (credentialsProvider == null)
			builder.credentialsProvider(AnonymousCredentialsProvider.create());
		else
			builder.credentialsProvider(credentialsProvider);

		if( clientBuilder != null )
			builder.httpClientBuilder(clientBuilder);

		// TODO figure out endpoint
		if (endpoint != null)
			builder.endpointOverride(endpoint.url());
		else if (region != null)
			builder.region(region);
		else
			builder.region(Region.US_EAST_1);

		final S3Client s3 = builder.build();
		// try to listBucket if we are anonymous, if we cannot, don't use anonymous.
		if (credentialsProvider == null) {

			// I initially tried checking whether the bucket exists, but
			// that, apparently, returns even when the client does not have access
			if (!AmazonS3Utils.bucketExists(s3, bucketName) || !canListBucket(s3, bucketName)) {
				// bucket not detected with anonymous credentials, try detecting credentials
				// and return it even if it can't detect the bucket, since there's nothing else to do
				builder.credentialsProvider(DefaultCredentialsProvider.create());
				return builder.build();
			}
		}

		return s3;
	}

	private static boolean canListBucket(final S3Client s3, final String bucket) {

		ListObjectsV2Request request = ListObjectsV2Request.builder()
				.bucket(bucket)
				.maxKeys(1)
				.build();

		try {
			// TODO validate this
			// list objects will throw an AmazonS3Exception (Access Denied) if this client does not have access
			s3.listObjectsV2(request);
			return true;
		} catch (final S3Exception e) {
			return false;
		}
	}
}
