package org.janelia.saalfeldlab.n5.s3;

import java.io.IOException;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for testing Amazon Web Services N5 implementation.
 * Tests that are specific to AWS S3 can be added here.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public abstract class AbstractN5AmazonS3Test extends AbstractN5Test {

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
