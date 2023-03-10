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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.GsonN5Writer;
import org.janelia.saalfeldlab.n5.N5URL;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * Amazon Web Services S3-based N5 implementation with version compatibility check.
 *
 * @author Igor Pisarev
 */
public class N5AmazonS3Writer extends N5AmazonS3Reader implements GsonN5Writer {

	/**
	 * Opens an {@link N5AmazonS3Writer} using an {@link AmazonS3} client and a given bucket name.
	 *
	 * @param s3
	 * @param bucketName
	 * @throws IOException
	 */
	public N5AmazonS3Writer(final AmazonS3 s3, final String bucketName) throws IOException {

		this(s3, bucketName, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5AmazonS3Writer} using an {@link AmazonS3} client, a given bucket name,
	 * and a path to the container within the bucket.
	 *
	 * @param s3
	 * @param bucketName
	 * @param containerPath
	 * @throws IOException
	 */
	public N5AmazonS3Writer(final AmazonS3 s3, final String bucketName, final String containerPath) throws IOException {

		this(s3, bucketName, containerPath, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5AmazonS3Writer} using an {@link AmazonS3} client and a given S3 URI.
	 *
	 * @param s3
	 * @param containerURI
	 * @throws IOException
	 */
	public N5AmazonS3Writer(final AmazonS3 s3, final AmazonS3URI containerURI) throws IOException {

		this(s3, containerURI, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5AmazonS3Writer} using an {@link AmazonS3} client and a given S3 URI
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * @param s3
	 * @param containerURI
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5AmazonS3Writer(final AmazonS3 s3, final AmazonS3URI containerURI, final GsonBuilder gsonBuilder) throws IOException {

		this(s3, containerURI.getBucket(), containerURI.getKey(), gsonBuilder);
	}

	/**
	 * Opens an {@link N5AmazonS3Writer} using an {@link AmazonS3} client and a given bucket name
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * @param s3
	 * @param bucketName
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5AmazonS3Writer(final AmazonS3 s3, final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		this(s3, bucketName, "/", gsonBuilder);
	}

	/**
	 * Opens an {@link N5AmazonS3Writer} using an {@link AmazonS3} client, a given bucket name,
	 * and a path to the container within the bucket with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * @param s3
	 * @param bucketName
	 * @param containerPath
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5AmazonS3Writer(
			final AmazonS3 s3,
			final String bucketName,
			final String containerPath,
			final GsonBuilder gsonBuilder) throws IOException {

		super(s3, getOrCreateBucketAndContainerPath(s3, bucketName, containerPath), containerPath, gsonBuilder);

		if (!VERSION.equals(getVersion()))
			setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	private static String getOrCreateBucketAndContainerPath(AmazonS3 s3, String bucketName, String containerPath) {

		if (!s3.doesBucketExistV2(bucketName))
			s3.createBucket(bucketName);

		final String rootPath = "/";
		if (!exists(s3, bucketName, containerPath, rootPath))
			createGroup(s3, bucketName, containerPath, rootPath);

		return bucketName;
	}

	@Override
	public void createGroup(final String pathName) throws IOException{

		createGroup(s3, bucketName, containerPath, pathName);
	}

	private static void createGroup(final AmazonS3 s3, final String bucketName, final String containerPath, final String pathName) {

		final Path groupPath = Paths.get(removeLeadingSlash(pathName));
		for (int i = 0; i < groupPath.getNameCount(); ++i) {
			final String parentGroupPath = groupPath.subpath(0, i + 1).toString();
			final String fullParentGroupPath = getFullPath(containerPath, parentGroupPath);
			final ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(0);
			s3.putObject(
					bucketName,
					replaceBackSlashes(addTrailingSlash(removeLeadingSlash(fullParentGroupPath))),
					new ByteArrayInputStream(new byte[0]),
					metadata);
		}
	}

	@Override public <T> void setAttribute(String pathName, String key, T attribute) throws IOException {

		JsonElement attributesRoot = getAttributes(pathName);
		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			final String attributePath = N5URL.normalizeAttributePath(key);
			GsonN5Writer.writeAttribute(new OutputStreamWriter(byteStream), attributesRoot, attributePath, attribute, getGson());
			writeS3Object(getAttributesKey(pathName), byteStream.toByteArray());
		}
	}

	@Override
	public void setAttributes(
			final String pathName,
			final Map<String, ?> attributes) throws IOException {

		JsonElement root = getAttributes(pathName);
		root = GsonN5Writer.insertAttributes(root, attributes, gson);

		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			GsonN5Writer.writeAttributes(new OutputStreamWriter(byteStream), root, gson);
			writeS3Object(getAttributesKey(pathName), byteStream.toByteArray());
		}
	}

	@Override public boolean removeAttribute(String pathName, String key) throws IOException {

		final JsonElement root = getAttributes(pathName);
		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			final boolean removed = GsonN5Writer.removeAttribute(new OutputStreamWriter(byteStream), root, N5URL.normalizeAttributePath(key), gson);
			if (removed) {
				writeS3Object(getAttributesKey(pathName), byteStream.toByteArray());
			}
			return removed;
		}
	}

	@Override public <T> T removeAttribute(String pathName, String key, Class<T> cls) throws IOException {

		final JsonElement root = getAttributes(pathName);
		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			final T removed = GsonN5Writer.removeAttribute(new OutputStreamWriter(byteStream), root, N5URL.normalizeAttributePath(key), cls, gson);
			if (removed != null) {
				writeS3Object(getAttributesKey(pathName), byteStream.toByteArray());
			}
			return removed;
		}
	}

	@Override public boolean removeAttributes(String pathName, List<String> attributes) throws IOException {

		final JsonElement root = getAttributes(pathName);
		boolean removed = false;
		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			final OutputStreamWriter writer = new OutputStreamWriter(byteStream);
			for (final String attribute : attributes) {
				removed |= GsonN5Writer.removeAttribute(root, N5URL.normalizeAttributePath(attribute), JsonElement.class, gson) != null;
			}
			if (removed) {
				GsonN5Writer.writeAttributes(writer, root, gson);
				writeS3Object(getAttributesKey(pathName), byteStream.toByteArray());
			}
			return removed;
		}
	}

	@Override
	public <T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			DefaultBlockWriter.writeBlock(byteStream, datasetAttributes, dataBlock);
			writeS3Object(getDataBlockKey(pathName, dataBlock.getGridPosition()), byteStream.toByteArray());
		}
	}

	@Override
	public boolean deleteBlock(final String pathName, final long... gridPosition) {

		final String dataBlockKey = getDataBlockKey(pathName, gridPosition);
		if (s3.doesObjectExist(bucketName, dataBlockKey))
			s3.deleteObject(bucketName, dataBlockKey);
		return !s3.doesObjectExist(bucketName, dataBlockKey);
	}

	@Override
	public boolean remove() throws IOException {

		final boolean wasPathRemoved = remove("/");
		if (!isContainerBucketRoot() || !wasPathRemoved)
			return wasPathRemoved;

		// N5 container was at the root level of the bucket so the bucket needs to be removed as well
		s3.deleteBucket(bucketName);
		return !s3.doesBucketExistV2(bucketName);
	}

	@Override
	public boolean remove(final String pathName) throws IOException {

		final String fullPath = getFullPath(pathName);
		final String prefix = fullPath.isEmpty() ? "" : addTrailingSlash(fullPath);
		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName(bucketName)
				.withPrefix(prefix);
		ListObjectsV2Result objectsListing;
		do {
			objectsListing = s3.listObjectsV2(listObjectsRequest);
			final List<String> objectsToDelete = new ArrayList<>();
			for (final S3ObjectSummary object : objectsListing.getObjectSummaries())
				objectsToDelete.add(object.getKey());

			if (!objectsToDelete.isEmpty()) {
				s3.deleteObjects(new DeleteObjectsRequest(bucketName)
						.withKeys(objectsToDelete.toArray(new String[objectsToDelete.size()])));
			}
			listObjectsRequest.setContinuationToken(objectsListing.getNextContinuationToken());
		} while (objectsListing.isTruncated());
		return !exists(pathName);
	}

	protected void writeS3Object(
			final String objectKey,
			final byte[] bytes) throws IOException {

		final ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(bytes.length);

		try (final InputStream data = new ByteArrayInputStream(bytes)) {
			s3.putObject(bucketName, objectKey, data, objectMetadata);
		}
	}
}
