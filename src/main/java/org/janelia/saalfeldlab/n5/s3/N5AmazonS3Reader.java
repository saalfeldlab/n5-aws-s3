package org.janelia.saalfeldlab.n5.s3;

import com.google.gson.GsonBuilder;

import software.amazon.awssdk.services.s3.S3Client;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;

/**
 *
 * @deprecated This class is deprecated and may be removed in a future release.
 * 	Replace with either `N5Factory.openReader()` or `N5KeyValueAccessReader` with
 * 	an `AmazonS3KeyValueAccess` backend.
 */
@Deprecated
public class N5AmazonS3Reader extends N5KeyValueReader {

	/**
	 * Opens an {@link N5Reader} with an {@link S3Client} storage backend.
	 *
	 * @deprecated This class is deprecated and may be removed in a future release.
	 * 	Replace with either `N5Factory.openReader()` or `N5KeyValueAccessReader` with
	 * 	an `AmazonS3KeyValueAccess` backend.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
     * @param gsonBuilder a GsonBuilder with custom configuration.
     * @param cacheMeta
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the reader could not be created
	 */
	@Deprecated
	public N5AmazonS3Reader(final S3Client s3, final String bucketName, final String basePath, final GsonBuilder gsonBuilder, final boolean cacheMeta) throws N5Exception {

		super(
				new AmazonS3KeyValueRoot(s3, bucketName, basePath, false),
				gsonBuilder,
				cacheMeta);

		if( !exists("/"))
			throw new N5Exception.N5IOException("No container exists at " + basePath );
	}

	/**
	 * Opens an {@link N5Reader} with an {@link S3Client} storage backend.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
     * @param cacheMeta
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5AmazonS3Reader(final S3Client s3, final String bucketName, final String basePath, final boolean cacheMeta) throws N5Exception {

		this(s3, bucketName, basePath, new GsonBuilder(), cacheMeta);
	}

	/**
	 * Opens an {@link N5Reader} with an {@link S3Client} storage backend.
     * <p>
     * Metadata are not cached.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
     * @param gsonBuilder a GsonBuilder with custom configuration.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5AmazonS3Reader(final S3Client s3, final String bucketName, final String basePath, final GsonBuilder gsonBuilder) throws N5Exception {

		this(s3, bucketName, basePath, gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5Reader} with an {@link S3Client} storage backend.
     * <p>
     * Metadata are not cached.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
	 * @throws N5Exception if the reader could not be created
	 */
	public N5AmazonS3Reader(final S3Client s3, final String bucketName, final String basePath) throws N5Exception {

		this(s3, bucketName, basePath, new GsonBuilder(), false);
	}

	/**
	 * Opens an {@link N5Reader} with an {@link S3Client} storage backend.
     * <p>
     * The n5 container root is the bucket's root.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param gsonBuilder a GsonBuilder with custom configuration.
     * @param cacheMeta
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5AmazonS3Reader(final S3Client s3, final String bucketName, final GsonBuilder gsonBuilder, final boolean cacheMeta) throws N5Exception {

		this(s3, bucketName, "/", gsonBuilder, cacheMeta);
	}

	/**
	 * Opens an {@link N5Reader} with an {@link S3Client} storage backend.
     * <p>
     * The n5 container root is the bucket's root.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param cacheMeta
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5AmazonS3Reader(final S3Client s3, final String bucketName, final boolean cacheMeta) throws N5Exception {

		this(s3, bucketName, "/", new GsonBuilder(), cacheMeta);
	}

	/**
	 * Opens an {@link N5Reader} with an {@link S3Client} storage backend.
     * <p>
     * The n5 container root is the bucket's root. Metadata are not cached.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param gsonBuilder a GsonBuilder with custom configuration.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5AmazonS3Reader(final S3Client s3, final String bucketName, final GsonBuilder gsonBuilder) throws N5Exception {

		this(s3, bucketName, "/", gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5Reader} with an {@link S3Client} storage backend.
     * <p>
     * The n5 container root is the bucket's root. Metadata are not cached.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
	 * @throws N5Exception if the reader could not be created
	 */
	public N5AmazonS3Reader(final S3Client s3, final String bucketName) throws N5Exception {

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
