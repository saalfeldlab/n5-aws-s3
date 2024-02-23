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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.s3.backend.BackendS3Factory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.google.gson.GsonBuilder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.janelia.saalfeldlab.n5.s3.AmazonS3Utils.getS3Bucket;
import static org.janelia.saalfeldlab.n5.s3.AmazonS3Utils.getS3Key;

/**
 * Base class for testing Amazon Web Services N5 implementation.
 * Tests that are specific to AWS S3 can be added here.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
@RunWith(Parameterized.class)
public class N5AmazonS3Tests extends AbstractN5Test {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {

		return Arrays.asList(new Object[][]{
				{"backend s3, container at generated path", null, false},
				{"backend s3, container at generated path , cache attributes", null, true},
				{"backend s3, container at root", "/", false},
				{"backend s3, container at root with , cache attributes", "/", true}
		});
	}

	private static int DOESNT_EXISTS_S3_CODE = 404;
	protected static HashMap<AmazonS3, ArrayList<String>> s3Buckets = new HashMap<>();
	private static final SecureRandom random = new SecureRandom();

	private final String testBucket = tempBucketName(getS3());

	@Parameterized.Parameter(0)
	public String name;

	@Parameterized.Parameter(1)
	public String tempPath;

	@Parameterized.Parameter(2)
	public boolean useCache;

	private static String generateName(final String prefix, final String suffix) {

		return prefix + Long.toUnsignedString(random.nextLong()) + suffix;
	}

	public static String tempBucketName(final AmazonS3 s3) {

		final String bucket = generateName("n5-test-", "-bucket");
		final ArrayList<String> s3Resources = s3Buckets.getOrDefault(s3, new ArrayList<>());
		s3Resources.add(bucket);
		s3Buckets.putIfAbsent(s3, s3Resources);
		return bucket;
	}

	public static String tempContainerPath() {

		return generateName("/n5-test-", ".n5");
	}

	@AfterClass
	public static void cleanup() {

		synchronized (s3Buckets) {
			for (Map.Entry<AmazonS3, ArrayList<String>> s3Buckets : s3Buckets.entrySet()) {
				final AmazonS3 s3 = s3Buckets.getKey();
				final ArrayList<String> buckets = s3Buckets.getValue();
				for (String bucket : buckets) {
					try {
						if (s3.doesBucketExistV2(bucket))
							s3.deleteBucket(bucket);
					} catch (AmazonS3Exception e) {
						if (e.getStatusCode() != DOESNT_EXISTS_S3_CODE)
							throw e;
					}
				}
			}
			s3Buckets.clear();
		}
	}

	protected AmazonS3 getS3() {

		return BackendS3Factory.getOrCreateS3();
	}

	@Override
	protected String tempN5Location() throws URISyntaxException {

		final String containerPath;
		if (tempPath != null)
			containerPath = tempPath;
		else
			containerPath = tempContainerPath();
		return new URI("s3", testBucket, containerPath, null).toString();
	}

	@Override protected N5Writer createN5Writer() throws URISyntaxException {

		final String location = tempN5Location();
		final String bucketName = getS3Bucket(location);
		final String basePath = getS3Key(location);
		return new N5AmazonS3Writer(getS3(), bucketName, basePath, new GsonBuilder(), useCache) {

			{
				final boolean localS3 = getS3().getUrl(bucketName, basePath).getAuthority().contains("localhost");
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

			@Override public void close() {

				remove();
				super.close();
			}
		};
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) {

		final String bucketName = getS3Bucket(location);
		final String basePath = getS3Key(location);
		return new N5AmazonS3Writer(getS3(), bucketName, basePath, gson);
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) {

		final String bucketName = getS3Bucket(location);
		final String basePath = getS3Key(location);
		return new N5AmazonS3Reader(getS3(), bucketName, basePath, gson);
	}

	/**
	 * Currently, {@code N5AmazonS3Reader#exists(String)} is implemented by listing objects under that group.
	 * This test case specifically tests its correctness.
	 *
	 * @throws IOException
	 */
	@Test
	public void testExistsUsingListingObjects() throws IOException, URISyntaxException {

		try (N5Writer n5 = createN5Writer()) {
			n5.createGroup("/one/two/three");

			Assert.assertTrue(n5.exists(""));
			Assert.assertTrue(n5.exists("/"));

			Assert.assertTrue(n5.exists("one"));
			Assert.assertTrue(n5.exists("one/"));
			Assert.assertTrue(n5.exists("/one"));
			Assert.assertTrue(n5.exists("/one/"));

			Assert.assertTrue(n5.exists("one/two"));
			Assert.assertTrue(n5.exists("one/two/"));
			Assert.assertTrue(n5.exists("/one/two"));
			Assert.assertTrue(n5.exists("/one/two/"));

			Assert.assertTrue(n5.exists("one/two/three"));
			Assert.assertTrue(n5.exists("one/two/three/"));
			Assert.assertTrue(n5.exists("/one/two/three"));
			Assert.assertTrue(n5.exists("/one/two/three/"));

			Assert.assertFalse(n5.exists("one/tw"));
			Assert.assertFalse(n5.exists("one/tw/"));
			Assert.assertFalse(n5.exists("/one/tw"));
			Assert.assertFalse(n5.exists("/one/tw/"));

			Assert.assertArrayEquals(new String[]{"one"}, n5.list("/"));
			Assert.assertArrayEquals(new String[]{"two"}, n5.list("/one"));
			Assert.assertArrayEquals(new String[]{"three"}, n5.list("/one/two"));

			Assert.assertArrayEquals(new String[]{}, n5.list("/one/two/three"));
			Assert.assertThrows(N5Exception.N5IOException.class, () -> n5.list("/one/tw"));

			Assert.assertTrue(n5.remove("/one/two/three"));
			Assert.assertFalse(n5.exists("/one/two/three"));
			Assert.assertTrue(n5.exists("/one/two"));
			Assert.assertTrue(n5.exists("/one"));

			Assert.assertTrue(n5.remove("/one"));
			Assert.assertFalse(n5.exists("/one/two"));
			Assert.assertFalse(n5.exists("/one"));
		}
	}
}
