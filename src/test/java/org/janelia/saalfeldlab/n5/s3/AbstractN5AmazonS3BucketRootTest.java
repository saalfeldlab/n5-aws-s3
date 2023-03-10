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
import java.nio.file.Paths;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.AfterClass;

import com.amazonaws.services.s3.AmazonS3;

public abstract class AbstractN5AmazonS3BucketRootTest extends AbstractN5AmazonS3Test {

    public AbstractN5AmazonS3BucketRootTest(final AmazonS3 s3) {

        super(s3);
    }

    @Override
    protected N5Writer createN5Writer() throws IOException {

        final String bucketName = tempBucketName();
        return new N5AmazonS3Writer(s3, bucketName);
    }

    @Override protected N5Writer createN5Writer(String location) throws IOException {

        return new N5AmazonS3Writer(s3, Paths.get(location).getFileName().toString());
    }

    @Override protected N5Writer createN5Writer(String location, GsonBuilder gson) throws IOException {

        return new N5AmazonS3Writer(s3, Paths.get(location).getFileName().toString(), gson);

    }

    @Override protected N5Reader createN5Reader(String location, GsonBuilder gson) throws IOException {

        return new N5AmazonS3Reader(s3, Paths.get(location).getFileName().toString(), gson);
    }

    @AfterClass
    public static void cleanup() throws IOException {

        rampDownAfterClass();
    }
}
