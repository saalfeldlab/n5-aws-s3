package org.janelia.saalfeldlab.n5.s3;

import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;

import com.google.gson.GsonBuilder;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * This class is used to create an N5Writer with an Amazon S3 storage backend.
 *
 * @deprecated This class is deprecated and may be removed in a future release.
 * 	Replace with either `N5Factory.openWriter()` or `N5KeyValueAccessWriter` with
 * 	an `AmazonS3KeyValueAccess` backend.
 */
@Deprecated
public class N5AmazonS3Writer extends N5KeyValueWriter {

	/**
	 * Opens an {@link N5Writer} with an {@link S3Client} storage backend.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
     * @param gsonBuilder a GsonBuilder with custom configuration.
     * @param cacheAttributes
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the writer could not be created
	 */
	public N5AmazonS3Writer(final S3Client s3, final String bucketName, final String basePath, final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws N5Exception {

		super(
				new AmazonS3KeyValueRoot(s3, bucketName,basePath, true),
				gsonBuilder,
				cacheAttributes);
	}

	/**
	 * Opens an {@link N5Writer} with an {@link S3Client} storage backend.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
     * @param cacheAttributes
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the writer could not be created
	 */
	public N5AmazonS3Writer(final S3Client s3, final String bucketName, final String basePath, final boolean cacheAttributes) throws N5Exception {

		this(s3, bucketName, basePath, new GsonBuilder(), cacheAttributes);
	}

	/**
	 * Opens an {@link N5Writer} with an {@link S3Client} storage backend.
	 * <p>
	 * Metadata are not cached.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
     * @param gsonBuilder a GsonBuilder with custom configuration.
	 * @throws N5Exception if the writer could not be created
	 */
	public N5AmazonS3Writer(final S3Client s3, final String bucketName, final String basePath, final GsonBuilder gsonBuilder) throws N5Exception {

		this(s3, bucketName, basePath, gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5Writer} with an {@link S3Client} storage backend.
	 * <p>
	 * Metadata are not cached.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
	 * @throws N5Exception if the writer could not be created
	 */
	public N5AmazonS3Writer(final S3Client s3, final String bucketName, final String basePath) throws N5Exception {

		this(s3, bucketName, basePath, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5Writer} with an {@link S3Client} storage backend.
	 * <p>
	 * The n5 container root is the bucket's root.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param gsonBuilder a GsonBuilder with custom configuration.
     * @param cacheAttributes
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the writer could not be created
	 */
	public N5AmazonS3Writer(final S3Client s3, final String bucketName, final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws N5Exception {

		this(s3, bucketName, "/", gsonBuilder, cacheAttributes);
	}

	/**
	 * Opens an {@link N5Writer} with an {@link S3Client} storage backend.
	 * <p>
	 * The n5 container root is the bucket's root.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param cacheAttributes
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the writer could not be created
	 */
	public N5AmazonS3Writer(final S3Client s3, final String bucketName, final boolean cacheAttributes) throws N5Exception {

		this(s3, bucketName, "/", new GsonBuilder(), cacheAttributes);
	}

	/**
	 * Opens an {@link N5Writer} with an {@link S3Client} storage backend.
	 * <p>
	 * The n5 container root is the bucket's root. Metadata are not cached.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
     * @param gsonBuilder a GsonBuilder with custom configuration.
	 * @throws N5Exception if the writer could not be created
	 */
	public N5AmazonS3Writer(final S3Client s3, final String bucketName, final GsonBuilder gsonBuilder) throws N5Exception {

		this(s3, bucketName, "/", gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5Writer} with an {@link S3Client} storage backend.
	 * <p>
	 * The n5 container root is the bucket's root. Metadata are not cached.
     *
     * @param s3 the amazon s3 instance
     * @param bucketName the bucket name
	 * @throws N5Exception if the writer could not be created
	 */
	public N5AmazonS3Writer(final S3Client s3, final String bucketName) throws N5Exception {

		this(s3, bucketName, "/", new GsonBuilder());
	}
}
