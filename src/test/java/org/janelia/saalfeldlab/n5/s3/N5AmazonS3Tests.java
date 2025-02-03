/*-
 * #%L
 * N5 AWS S3
 * %%
 * Copyright (C) 2017 - 2022, Saalfeld Lab
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.s3;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.s3.backend.BackendS3Factory;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.Statement;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import static org.janelia.saalfeldlab.n5.s3.AmazonS3Utils.getS3Bucket;
import static org.junit.Assume.assumeTrue;

/**
 * Base class for testing Amazon Web Services N5 implementation.
 * Tests that are specific to AWS S3 can be added here.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
@RunWith(Parameterized.class)
public class N5AmazonS3Tests extends AbstractN5Test {

	public static class SkipErroneousNoSuchBucketFailure extends TestWatcher {

		private void assumeFailIfNoSuchBucket(Throwable exception) {

			if (exception instanceof AwsServiceException)
				assumeFailIfNoSuchBucket(((AwsServiceException)exception));
			else if (exception.getCause() instanceof AwsServiceException)
				assumeFailIfNoSuchBucket(((AwsServiceException)exception.getCause()));
		}

		private void assumeFailIfNoSuchBucket(AwsServiceException exception) {

			final int statusCode = exception.awsErrorDetails().sdkHttpResponse().statusCode();
			final String errorCode = exception.awsErrorDetails().errorCode();
			if (errorCode == null) {
				throw exception;
			}
			assumeTrue("Erroneous NoSuchBucket Exception. Rerun to verify if this is a true test failure", statusCode != 404 && errorCode.equals("NoSuchBucket"));
		}

		@Override
		public Statement apply(final Statement base, final Description description) {

			try {
				base.evaluate();
			} catch (N5Exception | AwsServiceException exception) {
				assumeFailIfNoSuchBucket(exception);
				throw exception;
			} catch (Throwable ignore) {
			}
			return base;
		}
	}

	public enum LocationInBucket {
		ROOT(() -> "/", N5AmazonS3Tests::tempBucketName),
		KEY(N5AmazonS3Tests::tempContainerPath, tempBucketName()::toString);

		public final Supplier<String> getContainerPath;
		private final Supplier<String> getBucketName;

		LocationInBucket(Supplier<String> tempContainerPath, Supplier<String> tempBucketaName) {

			this.getContainerPath = tempContainerPath;
			this.getBucketName = tempBucketaName;
		}

		String getPath() {

			return getContainerPath.get();
		}

		String getBucketName() {

			return getBucketName.get();
		}
	}

	public enum UseCache {
		CACHE(true),
		NO_CACHE(false);

		final boolean cache;

		UseCache(boolean cache) {

			this.cache = cache;
		}
	}

	@Parameterized.Parameters(name = "Container at {0}, {1}")
	public static Collection<Object[]> data() {

		return Arrays.asList(new Object[][]{
				{LocationInBucket.ROOT, UseCache.NO_CACHE},
				{LocationInBucket.ROOT, UseCache.CACHE},
				{LocationInBucket.KEY, UseCache.NO_CACHE},
				{LocationInBucket.KEY, UseCache.CACHE}
		});
	}

	private static final SecureRandom random = new SecureRandom();

	protected static boolean skipErroneousBackendFailures = true;

	@Rule
	public TestWatcher skipErroneousWatcher = null;

	@Parameterized.Parameter()
	public LocationInBucket containerLocation;

	@Parameterized.Parameter(1)
	public UseCache useCache;

	protected static S3Client lateinitS3 = null;

	@Parameterized.AfterParam()
	public static void removeTestBuckets() {

		for (LocationInBucket location : LocationInBucket.values()) {
			final String bucketName = location.getBucketName();
			try {
				final AmazonS3KeyValueAccess kva = new AmazonS3KeyValueAccess(lateinitS3, N5URI.encodeAsUri("s3://" + bucketName), false);
				kva.delete(kva.normalize("/"));
			} catch (Exception e) {
				if (!lateinitS3.doesBucketExistV2(bucketName))
					continue;
				System.err.println("Exception After Tests, Could Not Delete Test Bucket:" + bucketName);
				e.printStackTrace();
			}
		}
	}

	private static String generateName(final String prefix, final String suffix) {

		return prefix + Long.toUnsignedString(random.nextLong()) + suffix;
	}

	public static String tempBucketName() {

		return generateName("n5-test-", "-bucket");
	}

	public static String tempContainerPath() {

		return generateName("/n5-test-", ".n5");
	}

	{
		if (skipErroneousBackendFailures)
			skipErroneousWatcher = new SkipErroneousNoSuchBucketFailure();
		lateinitS3 = getS3();
	}

	protected S3Client getS3() {

		return BackendS3Factory.getOrCreateS3();
	}

	@Override
	protected String tempN5Location() throws URISyntaxException {

		final String containerPath = containerLocation.getPath();
		final String testBucket = containerLocation.getBucketName();
		return new URI("s3", testBucket, containerPath, null).toString();
	}

	@Override protected N5Writer createN5Writer() throws URISyntaxException {

		final String s3ContainerUri = tempN5Location();

		return delayedBucketCreationWriter(s3ContainerUri, new GsonBuilder());
	}

	private N5KeyValueWriter delayedBucketCreationWriter(String s3ContainerUri, GsonBuilder gson) {

		final String bucketName = getS3Bucket(s3ContainerUri);

		final KeyValueAccess s3kva = new AmazonS3KeyValueAccess(getS3(), URI.create(s3ContainerUri), true);

		return new N5KeyValueWriter(s3kva, s3ContainerUri, gson, useCache.cache) {

			{
				final URI containerUri;
				try {
					containerUri = s3kva.uri("");
				} catch (URISyntaxException e) {
					throw new RuntimeException(e);
				}
				final boolean localS3 = containerUri.getAuthority().contains("localhost");
				if (!localS3) {
					/* Creating a bucket on S3 only provides a guarantee of eventual consistency. To
					 * ensure the bucket is created before testing, we wait to ensure it's visible before continuing.
					 * https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel */
					int timeoutMs = 5 * 1000;
					while (timeoutMs > 0) {
						if (getS3().doesBucketExistV2(bucketName))
							break;
						else
							try {
								Thread.sleep(100);
								timeoutMs -= 100;
							} catch (final InterruptedException e) {
								e.printStackTrace();
							}
					}
					if (timeoutMs < 0)
						throw new RuntimeException("Attempt to create bucket and wait for consistency failed.");
				}
			}
		};
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) {

		return delayedBucketCreationWriter(location, gson);
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) {

		final KeyValueAccess s3kva;
		try {
			s3kva = new AmazonS3KeyValueAccess(getS3(), new URI(location), false);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}

		return new N5KeyValueReader(s3kva, location, gson, useCache.cache);
	}

	@Test
	@Override
	public void testWriterSeparation() {

		/* The base test will fail when `container is bucket root` parameter is true; Skip the test in that case. */
		assumeTrue("Writer Separation fails when container is at the bucket root, since the writers are at the same location", containerLocation != LocationInBucket.ROOT);
	}

	@Test
	@Ignore("This seems not to work as expected when run in maven specifically")
	public void testErroneousNoSuchBucketFailure() {

		throw new S3Exception(
				"This Exception should trigger a skipped test, not a failure",
				new S3Exception("Erroneous NoSuchBucket Failure") {

					{
						setErrorCode("NoSuchBucket");
						setStatusCode(404);
					}
				});

	}
//	public static void deleteAllTestBuckets() {
//
//		final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
//		for (Bucket bucket : s3.listBuckets()) {
//			final String bucketName = bucket.getName();
//			if (bucketName.startsWith("n5-test-")) {
//				final String containerURI = "s3://" + bucketName;
//				final N5KeyValueWriter writer = new N5KeyValueWriter(new AmazonS3KeyValueAccess(s3, containerURI, false), containerURI, new GsonBuilder(), false);
//				writer.remove();
//			}
//		}
//	}
}
