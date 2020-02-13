/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
