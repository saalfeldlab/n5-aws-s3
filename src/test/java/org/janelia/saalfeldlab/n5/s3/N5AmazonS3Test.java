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

import java.io.IOException;
import java.util.UUID;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.GsonAttributesParser;
import org.junit.BeforeClass;

/**
 * Initiates testing of the Amazon Web Services S3-based N5 implementation.
 *
 * Expects that security credentials (access key ID and secret key) are provided in one of the following sources:
 * - environment variables
 * - system (JVM) variables
 * - credentials profile file
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5AmazonS3Test extends AbstractN5Test {

	static private String testBucketName = "test-bucket-" + UUID.randomUUID();

	/**
	 * @throws IOException
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws IOException {

		n5 = N5AmazonS3.openS3Writer(testBucketName);
		n5Parser = (GsonAttributesParser)n5;

		AbstractN5Test.setUpBeforeClass();
	}
}
