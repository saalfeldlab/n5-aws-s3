/*-
 * #%L
 * N5 AWS S3
 * %%
 * Copyright (C) 2017 - 2022 Igor Pisarev, Stephan Saalfeld
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

import com.amazonaws.services.s3.AmazonS3;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * Base class for testing Amazon Web Services N5 implementation.
 * Tests that are specific to AWS S3 can be added here.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public abstract class AbstractN5AmazonS3Test extends AbstractN5Test {

	protected static String testBucketName = "n5-test-" + UUID.randomUUID();

	protected static AmazonS3 s3;

	public AbstractN5AmazonS3Test(final AmazonS3 s3) {

		AbstractN5AmazonS3Test.s3 = s3;
	}

	/**
	 * Currently, {@code N5AmazonS3Reader#exists(String)} is implemented by listing objects under that group.
	 * This test case specifically tests its correctness.
	 *
	 * @throws IOException
	 */
	@Test
	public void testExistsUsingListingObjects() throws IOException {

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

		Assert.assertArrayEquals(new String[] {"one"}, n5.list("/"));
		Assert.assertArrayEquals(new String[] {"two"}, n5.list("/one"));
		Assert.assertArrayEquals(new String[] {"three"}, n5.list("/one/two"));
		Assert.assertArrayEquals(new String[] {}, n5.list("/one/two/three"));
		Assert.assertArrayEquals(new String[] {}, n5.list("/one/tw"));

		Assert.assertTrue(n5.remove("/one/two/three"));
		Assert.assertFalse(n5.exists("/one/two/three"));
		Assert.assertTrue(n5.exists("/one/two"));
		Assert.assertTrue(n5.exists("/one"));

		Assert.assertTrue(n5.remove("/one"));
		Assert.assertFalse(n5.exists("/one/two"));
		Assert.assertFalse(n5.exists("/one"));
	}
}
