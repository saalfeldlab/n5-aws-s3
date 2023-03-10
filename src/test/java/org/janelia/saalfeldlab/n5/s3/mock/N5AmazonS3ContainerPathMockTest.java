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
package org.janelia.saalfeldlab.n5.s3.mock;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.s3.AbstractN5AmazonS3ContainerPathTest;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Reader;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Writer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/**
 * Initiates testing of the Amazon Web Services S3-based N5 implementation using S3 mock library.
 * A non-trivial container path is used to create the test N5 container in the temporary bucket.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5AmazonS3ContainerPathMockTest extends AbstractN5AmazonS3ContainerPathTest {

	public N5AmazonS3ContainerPathMockTest() {

		super(MockS3Factory.getOrCreateS3());
	}

	@Test
	@Override
	public void testReaderCreation() throws IOException {

		final String containerPath = tempN5PathName();
        final String bucketName = tempBucketName();
        try (N5Writer writer = new N5AmazonS3Writer(s3, bucketName, containerPath, new GsonBuilder())) {

			final N5Reader n5r = new N5AmazonS3Reader(s3, bucketName, containerPath, new GsonBuilder());
			assertNotNull(n5r);

			// existing directory without attributes is okay;
			// Remove and create to remove attributes store
			writer.remove("/");
			writer.createGroup("/");
			final N5Reader na = new N5AmazonS3Reader(s3, bucketName, containerPath, new GsonBuilder());
			assertNotNull(na);

			// existing location with attributes, but no version
			writer.remove("/");
			writer.createGroup("/");
			writer.setAttribute("/", "mystring", "ms");
			final N5Reader wa = new N5AmazonS3Reader(s3, bucketName, containerPath, new GsonBuilder());
			assertNotNull(wa);

			// existing directory with incompatible version should fail
			writer.remove("/");
			writer.createGroup("/");
			writer.setAttribute("/", N5Reader.VERSION_KEY,
					new N5Reader.Version(N5Reader.VERSION.getMajor() + 1, N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch()).toString());
			assertThrows("Incompatible version throws error", IOException.class,
					() -> {
						new N5AmazonS3Reader(s3, bucketName, containerPath, new GsonBuilder());
					});

			// non-existent directory should fail
			writer.remove("/");
			assertThrows("Non-existant location throws error", IOException.class,
					() -> {
						final N5Reader test = new N5AmazonS3Reader(s3, bucketName, containerPath, new GsonBuilder());
						test.list("/");
					});
		}
	}

}
