/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.janelia.saalfeldlab.n5.AbstractGsonReader;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.GsonAttributesParser;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * Amazon Web Services S3-based N5 implementation with version compatibility check.
 *
 * @author Igor Pisarev
 */
public class N5AmazonS3Reader extends AbstractGsonReader implements N5Reader {

	protected static final String jsonFile = "attributes.json";

	protected final AmazonS3 s3;
	protected final String bucketName;
	protected final String containerPath;

	/**
	 * Helper class that drains the rest of the {@link S3ObjectInputStream} on {@link #close()}.
	 *
	 * Without draining the stream AWS S3 SDK sometimes outputs the following warning message:
	 * "... Not all bytes were read from the S3ObjectInputStream, aborting HTTP connection ...".
	 *
	 * Draining the stream helps to avoid this warning and possibly reuse HTTP connections.
	 *
	 * Calling {@link S3ObjectInputStream#abort()} does not prevent this warning as discussed here:
	 * https://github.com/aws/aws-sdk-java/issues/1211
	 */
	private static class S3ObjectInputStreamDrain extends InputStream {

		private final S3ObjectInputStream in;
		private boolean closed;

		public S3ObjectInputStreamDrain(final S3ObjectInputStream in) {

			this.in = in;
		}

		@Override
		public int read() throws IOException {

			return in.read();
		}

		@Override
		public void close() throws IOException {

			if (!closed) {
				while (read() != -1);
				in.close();
				closed = true;
			}
		}
	}

	/**
	 * Opens an {@link N5AmazonS3Reader} using an {@link AmazonS3} client and a given bucket name.
	 *
	 * If the bucket does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3
	 * @param bucketName
	 * @throws IOException
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName) throws IOException {

		this(s3, bucketName, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5AmazonS3Reader} using an {@link AmazonS3} client, a given bucket name,
	 * and a path to the container within the bucket.
	 *
	 * If the bucket and/or container does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3
	 * @param bucketName
	 * @param containerPath
	 * @throws IOException
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName, final String containerPath) throws IOException {

		this(s3, bucketName, containerPath, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5AmazonS3Reader} using an {@link AmazonS3} client and a given S3 URI.
	 *
	 * If the bucket and/or container does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3
	 * @param containerURI
	 * @throws IOException
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final AmazonS3URI containerURI) throws IOException {

		this(s3, containerURI, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5AmazonS3Reader} using an {@link AmazonS3} client and a given S3 URI
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket and/or container does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3
	 * @param containerURI
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final AmazonS3URI containerURI, final GsonBuilder gsonBuilder) throws IOException {

		this(s3, containerURI.getBucket(), containerURI.getKey(), gsonBuilder);
	}

	/**
	 * Opens an {@link N5AmazonS3Reader} using an {@link AmazonS3} client and a given bucket name
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3
	 * @param bucketName
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5AmazonS3Reader(final AmazonS3 s3, final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		this(s3, bucketName, "/", gsonBuilder);
	}

	/**
	 * Opens an {@link N5AmazonS3Reader} using an {@link AmazonS3} client, a given bucket name,
	 * and a path to the container within the bucket with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket and/or container does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3
	 * @param bucketName
	 * @param containerPath
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5AmazonS3Reader(
			final AmazonS3 s3,
			final String bucketName,
			final String containerPath,
			final GsonBuilder gsonBuilder) throws IOException {

		super(gsonBuilder);

		this.s3 = s3;
		this.bucketName = bucketName;
		this.containerPath = containerPath;

		if (s3.doesBucketExistV2(bucketName)) {
			final Version version = getVersion();
			if (!VERSION.isCompatible(version))
				throw new IOException("Incompatible version " + version + " (this is " + VERSION + ").");
		}
	}

	@Override
	public boolean exists(final String pathName) {

		final String fullPath = getFullPath(pathName);
		final String prefix = fullPath.isEmpty() ? "" : addTrailingSlash(fullPath);
		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName(bucketName)
				.withPrefix(prefix)
				.withMaxKeys(1);
		final ListObjectsV2Result objectsListing = s3.listObjectsV2(listObjectsRequest);
		return objectsListing.getKeyCount() > 0;
	}

	@Override
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {

		final String attributesKey = getAttributesKey(pathName);
		if (!s3.doesObjectExist(bucketName, attributesKey))
			return new HashMap<>();

		try (final InputStream in = readS3Object(attributesKey)) {
			return GsonAttributesParser.readAttributes(new InputStreamReader(in), gson);
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		final String dataBlockKey = getDataBlockKey(pathName, gridPosition);
		if (!s3.doesObjectExist(bucketName, dataBlockKey))
			return null;

		try (final InputStream in = readS3Object(dataBlockKey)) {
			return DefaultBlockReader.readBlock(in, datasetAttributes, gridPosition);
		}
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final String fullPath = getFullPath(pathName);
		final String prefix = fullPath.isEmpty() ? "" : addTrailingSlash(fullPath);
		final Path path = Paths.get(prefix);

		final List<String> subGroups = new ArrayList<>();
		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName(bucketName)
				.withPrefix(prefix)
				.withDelimiter("/");
		ListObjectsV2Result objectsListing;
		do {
			objectsListing = s3.listObjectsV2(listObjectsRequest);
			for (final String commonPrefix : objectsListing.getCommonPrefixes()) {
				if (commonPrefix.endsWith("/")) {
					final Path relativePath = path.relativize(Paths.get(commonPrefix));
					final String correctedSubgroupPathName = replaceBackSlashes(relativePath.toString());
					if (!correctedSubgroupPathName.isEmpty())
						subGroups.add(correctedSubgroupPathName);
				}
			}
			listObjectsRequest.setContinuationToken(objectsListing.getNextContinuationToken());
		} while (objectsListing.isTruncated());
		return subGroups.toArray(new String[subGroups.size()]);
	}

	protected InputStream readS3Object(final String objectKey) throws IOException {

		final S3ObjectInputStream in = s3.getObject(bucketName, objectKey).getObjectContent();
		return new S3ObjectInputStreamDrain(in);
	}

	/**
	 * AWS S3 service accepts only forward slashes as path delimiters.
	 * This method replaces back slashes to forward slashes (if any) and returns a corrected path name.
	 *
	 * @param pathName
	 * @return
	 */
	protected static String replaceBackSlashes(final String pathName) {

		return pathName.replace("\\", "/");
	}

	/**
	 * When absolute paths are passed (e.g. /group/data), AWS S3 service creates an additional root folder with an empty name.
	 * This method removes the root slash symbol and returns the corrected path.
	 *
	 * Additionally, it ensures correctness on both Unix and Windows platforms, otherwise {@code pathName} is treated
	 * as UNC path on Windows, and {@code Paths.get(pathName, ...)} fails with {@code InvalidPathException}.
	 *
	 * @param pathName
	 * @return
	 */
	protected static String removeLeadingSlash(final String pathName) {

		return pathName.startsWith("/") || pathName.startsWith("\\") ? pathName.substring(1) : pathName;
	}

	/**
	 * When listing children objects for a group, must append a delimiter to the path (e.g. group/data/).
	 * This is necessary for not including wrong objects in the filtered set
	 * (e.g. group/data-2/attributes.json when group/data is passed without the last slash).
	 *
	 * @param pathName
	 * @return
	 */
	protected static String addTrailingSlash(final String pathName) {

		return pathName.endsWith("/") || pathName.endsWith("\\") ? pathName : pathName + "/";
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid position.
	 *
	 * The returned path is
	 * <pre>
	 * $datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 *
	 * This is the file into which the data block will be stored.
	 *
	 * @param datasetPathName
	 * @param gridPosition
	 * @return
	 */
	protected String getDataBlockKey(
			final String datasetPathName,
			final long... gridPosition) {

		final String[] pathComponents = new String[gridPosition.length];
		for (int i = 0; i < pathComponents.length; ++i)
			pathComponents[i] = Long.toString(gridPosition[i]);

		final String dataBlockPathName = Paths.get(removeLeadingSlash(datasetPathName), pathComponents).toString();
		return getFullPath(dataBlockPathName);
	}

	/**
	 * Constructs a full path for a path that is relative to the container.
	 *
	 * @param relativePath
	 * @return
	 */
	protected String getFullPath(final String relativePath) {

		final String fullPath = Paths.get(removeLeadingSlash(containerPath), relativePath).toString();
		return removeLeadingSlash(replaceBackSlashes(fullPath));
	}

	/**
	 * Constructs the path for the attributes file of a group or dataset.
	 *
	 * @param pathName
	 * @return
	 */
	protected String getAttributesKey(final String pathName) {

		final String attributesPath = Paths.get(removeLeadingSlash(pathName), jsonFile).toString();
		return getFullPath(attributesPath);
	}

	/**
	 * Determines whether the current N5 container is stored at the root level of the bucket.
	 *
	 * @return
	 */
	protected boolean isContainerBucketRoot() {
		return removeLeadingSlash(containerPath).isEmpty();
	}
}
