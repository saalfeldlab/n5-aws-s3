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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.GsonAttributesParser;
import org.janelia.saalfeldlab.n5.N5Writer;

import com.amazonaws.services.s3.AmazonS3;
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
 * Amazon S3 does not have conventional files and directories, instead it operates on objects with unique keys.
 * This implementation enforces that an empty attributes file is present for each group.
 * It is used for determining group existence and listing groups.
 *
 * @author Igor Pisarev
 */
public class N5AmazonS3Writer extends N5AmazonS3Reader implements N5Writer {

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

		super(s3, bucketName, gsonBuilder);

		if (!s3.doesBucketExistV2(bucketName))
			s3.createBucket(bucketName);

		setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	/**
	 * Opens an {@link N5AmazonS3Writer} using an {@link AmazonS3} client and a given bucket name.
	 *
	 * @param s3
	 * @param bucketName
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5AmazonS3Writer(final AmazonS3 s3, final String bucketName) throws IOException {

		this(s3, bucketName, new GsonBuilder());
	}

	@Override
	public void createGroup(final String pathName) throws IOException {

		final Path path = Paths.get(pathName);
		for (int i = 0; i < path.getNameCount(); ++i) {
			final String subgroup = path.subpath(0, i + 1).toString();
			if (!exists(subgroup))
				setAttributes(subgroup, Collections.emptyMap());
		}
	}

	@Override
	public void setAttributes(
			final String pathName,
			final Map<String, ?> attributes) throws IOException {

		final HashMap<String, JsonElement> map = getAttributes(pathName);
		GsonAttributesParser.insertAttributes(map, attributes, gson);

		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			GsonAttributesParser.writeAttributes(new OutputStreamWriter(byteStream), map, gson);
			writeS3Object(getAttributesKey(pathName), byteStream.toByteArray());
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
	public boolean remove() throws IOException {

		remove("");
		s3.deleteBucket(bucketName);
		return !s3.doesBucketExistV2(bucketName);
	}

	@Override
	public boolean remove(final String pathName) throws IOException {

		final String correctedPathName = removeFrontDelimiter(ensureCorrectDelimiter(pathName));
		final String prefix = correctedPathName.isEmpty() ? "" : appendDelimiter(correctedPathName);
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
						.withKeys(objectsToDelete.toArray(new String[objectsToDelete.size()]))
					);
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
