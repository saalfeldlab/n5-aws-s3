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
package org.janelia.saalfeldlab.n5.s3.backend;

import java.io.IOException;
import java.util.Map;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Writer;

import com.amazonaws.services.s3.AmazonS3;
import com.google.gson.GsonBuilder;

/**
 * Helper class for dealing with eventual consistency of S3 store.
 *
 * S3 store has a concept of eventual consistency: for example, when an object is requested after it has been overwritten,
 * it may still return the old version of the object, but eventually it will return the updated version.
 *
 * The tests in {@link AbstractN5Test} write and then immediately read attributes and data blocks to verify them.
 * Eventual consistency makes some of these tests fail. To solve or at least minimize this effect,
 * this class adds a 1s delay after each modification request.
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel">https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel</a>
 */
class N5AmazonS3DelayedWriter extends N5AmazonS3Writer {

    private static final long delayMsec = 1000;

    public N5AmazonS3DelayedWriter(final AmazonS3 s3, final String bucketName, final GsonBuilder gson, final boolean cacheAttributes) throws IOException {

        super(s3, bucketName, gson, cacheAttributes);
        sleep();
    }

	public N5AmazonS3DelayedWriter(final AmazonS3 s3, final String bucketName, final String basePath, final GsonBuilder gson, final boolean cacheAttributes) throws IOException {

		super(s3, bucketName, basePath, gson, cacheAttributes);
		sleep();
	}

    @Override
    public void createGroup(final String pathName) throws IOException {

        super.createGroup(pathName);
        sleep();
    }

    @Override
    public void setAttributes(
            final String pathName,
            final Map<String, ?> attributes) throws IOException {

        super.setAttributes(pathName, attributes);
        sleep();
    }

    @Override
    public <T> void writeBlock(
            final String pathName,
            final DatasetAttributes datasetAttributes,
            final DataBlock<T> dataBlock) throws IOException {

        super.writeBlock(pathName, datasetAttributes, dataBlock);
        sleep();
    }

    @Override
    public boolean deleteBlock(final String pathName, final long[] gridPosition) throws IOException {

        final boolean ret = super.deleteBlock(pathName, gridPosition);
        sleep();
        return ret;
    }

    @Override
    public boolean remove() throws IOException {

        final boolean ret = super.remove();
        sleep();
        return ret;
    }

    @Override
    public boolean remove(final String pathName) throws IOException {

        final boolean ret = super.remove(pathName);
        sleep();
        return ret;
    }

    static void sleep() {

        try {
            Thread.sleep(delayMsec);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }
}
