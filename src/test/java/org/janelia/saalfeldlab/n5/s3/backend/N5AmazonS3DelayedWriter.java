package org.janelia.saalfeldlab.n5.s3.backend;

import com.amazonaws.services.s3.AmazonS3;
import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Writer;

import org.janelia.saalfeldlab.n5.AbstractN5Test;

import java.io.IOException;
import java.util.Map;

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

    public N5AmazonS3DelayedWriter(final AmazonS3 s3, final String bucketName) throws IOException {

        super(s3, bucketName, new GsonBuilder());
        sleep();
    }

    public N5AmazonS3DelayedWriter(final AmazonS3 s3, final String bucketName, final String containerPath) throws IOException {

        super(s3, bucketName, containerPath, new GsonBuilder());
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
    public boolean deleteBlock(final String pathName, final long[] gridPosition) {

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
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
