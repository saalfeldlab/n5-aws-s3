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

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.google.gson.GsonBuilder;

/**
 * Base class for testing Amazon Web Services N5 implementation.
 * Tests that are specific to AWS S3 can be added here.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public abstract class AbstractN5AmazonS3Test extends AbstractN5Test {

	protected static AmazonS3 s3;

	public AbstractN5AmazonS3Test(final AmazonS3 s3) {

		AbstractN5AmazonS3Test.s3 = s3;
	}

	private static final SecureRandom random = new SecureRandom();

	private static String generateName(final String prefix, final String suffix) {

		return prefix + Long.toUnsignedString(random.nextLong()) + suffix;
	}

	protected static String tempBucketName() {

		return generateName("n5-test-", "-bucket");
	}

	protected static String tempContainerPath() {

		return generateName("/n5-test-", ".n5");
	}

	@Override protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		final String location = tempN5Location();
		final String bucketName = getS3Bucket( location );
		final String basePath = getS3Key(location);
		return new N5AmazonS3Writer(s3, bucketName, basePath, new GsonBuilder()) {

			@Override public void close() {

				remove();
				super.close();
			}
		};
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		final String bucketName = getS3Bucket(location);
		final String basePath = getS3Key(location);
		return new N5AmazonS3Writer(s3, bucketName, basePath, gson);
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		final String bucketName = getS3Bucket(location);
		final String basePath = getS3Key(location);
		return new N5AmazonS3Reader(s3, bucketName, basePath, gson);
	}

	protected String getS3Bucket(final String uri) {

		try {
			return new AmazonS3URI(uri).getBucket();
		} catch (final IllegalArgumentException e) {}
		try {
			// parse bucket manually when AmazonS3URI can't
			final String path = new URI(uri).getPath().replaceFirst("^/", "");
			return path.substring(0, path.indexOf('/'));
		} catch (final URISyntaxException e) {
		}
		return null;
	}

	protected String getS3Key(final String uri) {

		try {
			// if key is null, return the empty string
			final String key = new AmazonS3URI(uri).getKey();
			return key == null ? "" : key;
		} catch (final IllegalArgumentException e) {}
		try {
			// parse key manually when AmazonS3URI can't
			final String path = new URI(uri).getPath().replaceFirst("^/", "");
			return path.substring(path.indexOf('/') + 1);
		} catch (final URISyntaxException e) {}
		return "";
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
