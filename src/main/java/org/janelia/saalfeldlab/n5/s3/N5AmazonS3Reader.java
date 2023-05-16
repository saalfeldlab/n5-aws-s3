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

import com.amazonaws.services.s3.AmazonS3;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;

/**
 * TODO: javadoc
 */
public class N5AmazonS3Reader extends N5KeyValueReader {

	/**
	 * TODO: reduce number of constructors ?
	 */

	/**
	 * TODO: javadoc
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName, final String basePath, final GsonBuilder gsonBuilder, final boolean cacheMeta) throws IOException {

		super(
				new AmazonS3KeyValueAccess(s3, bucketName, false),
				basePath,
				gsonBuilder,
				cacheMeta);

		if( !exists("/"))
			throw new N5Exception.N5IOException("No container exists at " + basePath );
	}

	/**
	 * TODO: javadoc
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName, final String basePath, final boolean cacheMeta) throws IOException {

		this(s3, bucketName, basePath, new GsonBuilder(), cacheMeta);
	}

	/**
	 * TODO: javadoc
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName, final String basePath, final GsonBuilder gsonBuilder) throws IOException {

		this(s3, bucketName, basePath, gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName, final String basePath) throws IOException {

		this(s3, bucketName, basePath, new GsonBuilder(), false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName, final GsonBuilder gsonBuilder, final boolean cacheMeta) throws IOException {

		this(s3, bucketName, "/", gsonBuilder, cacheMeta);
	}

	/**
	 * TODO: javadoc
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName, final boolean cacheMeta) throws IOException {

		this(s3, bucketName, "/", new GsonBuilder(), cacheMeta);
	}

	/**
	 * TODO: javadoc
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		this(s3, bucketName, "/", gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName) throws IOException {

		this(s3, bucketName, "/", new GsonBuilder(), false);
	}


//	/**
//	 * Determines whether the current N5 container is stored at the root level of the bucket.
//	 *
//	 * @return
//	 */
//	protected boolean isContainerBucketRoot() {
//		return isContainerBucketRoot(containerPath);
//	}
//
//	protected static boolean isContainerBucketRoot(String containerPath) {
//		return removeLeadingSlash(containerPath).isEmpty();
//	}
}
