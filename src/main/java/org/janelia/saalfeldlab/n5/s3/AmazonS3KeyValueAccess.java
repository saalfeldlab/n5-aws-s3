/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.NonReadableChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.janelia.saalfeldlab.n5.N5URI;

public class AmazonS3KeyValueAccess implements KeyValueAccess {

	private final AmazonS3 s3;
	private final String bucketName;

	/**
	 * Opens an {@link AmazonS3KeyValueAccess} using an {@link AmazonS3} client and a given bucket name.
	 *
	 * If the bucket does not exist and {@code bucketName==true}, the bucket will be created.
	 * If the bucket does not exist and {@code bucketName==false}, the bucket will not be
	 * created and all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3
	 * @param bucketName
	 * @param createBucket whether {@code bucketName} should be created if it doesn't exist
	 * @throws IOException
	 */
	public AmazonS3KeyValueAccess(final AmazonS3 s3, final String bucketName, final boolean createBucket) throws N5Exception.N5IOException {

		this.s3 = s3;
		this.bucketName = bucketName;

		if (!s3.doesBucketExistV2(bucketName)) {
			if (createBucket) {
				Region region;
				try {
					region = s3.getRegion();
				} catch (final IllegalStateException e) {
					region = Region.US_Standard;
				}
				s3.createBucket(new CreateBucketRequest(bucketName, region));
			} else {
				throw new N5Exception.N5IOException("Bucket " + bucketName + " does not exist.");
			}
		}
	}

	@Override
	public String[] components(final String path) {

		return Arrays.stream(path.split("/"))
				.filter(x -> !x.isEmpty())
				.toArray(String[]::new);
	}

	@Override
	public String compose(final String... components) {

		if (components == null || components.length == 0)
			return null;

		return normalize(
				Arrays.stream(components)
				.filter(x -> !x.isEmpty())
				.collect(Collectors.joining("/"))
		);
	}

	@Override
	public String parent(final String path) {

		final String[] components = components(path);
		final String[] parentComponents =Arrays.copyOf(components, components.length - 1);

		return compose(parentComponents);
	}

	@Override
	public String relativize(final String path, final String base) {

		try {
			/* Must pass absolute path to `uri`. if it already is, this is redundant, and has no impact on the result.
			* 	It's not true that the inputs are always referencing absolute paths, but it doesn't matter in this
			* 	case, since we only care about the relative portion of `path` to `base`, so the result always
			* 	ignores the absolute prefix anyway. */
			return normalize(uri("/" + base).relativize(uri("/" + path)).getPath());
		} catch (URISyntaxException e) {
			throw new N5Exception("Cannot relativize path (" + path +") with base (" + base + ")", e);
		}
	}

	@Override
	public String normalize(final String path) {

		return N5URI.normalizeGroupPath(path);
	}

	@Override
	public URI uri(final String normalPath) throws URISyntaxException {

		return new URI("s3", bucketName, normalPath, null);
	}

	/**
	 * Test whether the {@code normalPath} exists.
	 * <p>
	 * Removes leading slash from {@code normalPath}, and then checks whether
	 * either {@code path} or {@code path + "/"} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 * 		efforts are made to normalize it.
	 * @return {@code true} if {@code path} exists, {@code false} otherwise
	 */
	@Override
	public boolean exists(final String normalPath) {

		return isDirectory(normalPath) || isFile(normalPath);
	}

	/**
	 * Find the smallest key with the given {@code prefix}.
	 *
	 * @return shortest key with the given {@code prefix}, or {@code null} if there is no key with that prefix.
	 */
	// TODO: REMOVE?
	private String shortestKeyWithPrefix(final String prefix) {

		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName(bucketName)
				.withPrefix(prefix)
				.withMaxKeys(1);
		final ListObjectsV2Result objectsListing = s3.listObjectsV2(listObjectsRequest);
		return objectsListing.getKeyCount() > 0
				? objectsListing.getObjectSummaries().get(0).getKey()
				: null;
	}

	/**
	 * Check existence of the given {@code key}.
	 *
	 * @return {@code true} if {@code key} exists.
	 */
	private boolean keyExists(final String key) {

		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName(bucketName)
				.withPrefix(key)
				.withMaxKeys(1);
		final ListObjectsV2Result objectsListing = s3.listObjectsV2(listObjectsRequest);
		if (objectsListing.getKeyCount() > 0) {
			final String k = objectsListing.getObjectSummaries().get(0).getKey();
			return k.length() == key.length();
		}
		return false;
	}

	/**
	 * When listing children objects for a group, must append a delimiter to the path (e.g. group/data/).
	 * This is necessary for not including wrong objects in the filtered set
	 * (e.g. group/data-2/attributes.json when group/data is passed without the last slash).
	 *
	 * @param path
	 * @return
	 */
	private static String addTrailingSlash(final String path) {

		return path.endsWith("/") ? path : path + "/";
	}

	/**
	 * When absolute paths are passed (e.g. /group/data), AWS S3 service creates an additional root folder with an empty name.
	 * This method removes the root slash symbol and returns the corrected path.
	 *
	 * @param path
	 * @return
	 */
	private static String removeLeadingSlash(final String path) {

		return path.startsWith("/") ? path.substring(1) : path;
	}

	/**
	 * Test whether the path is a directory.
	 * <p>
	 * Appends trailing "/" to {@code normalPath} if there is none, removes
	 * leading "/", and then checks whether resulting {@code path} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 * 		efforts are made to normalize it.
	 * @return {@code true} if {@code path} (with trailing "/") exists as a key, {@code false} otherwise
	 */
	@Override
	public boolean isDirectory(final String normalPath) {

		final String key = removeLeadingSlash(addTrailingSlash(normalPath));
		return key.isEmpty() || keyExists(key);
	}

	/**
	 * Test whether the path is a file.
	 * <p>
	 * Checks whether {@code normalPath} has no trailing "/", then removes
	 * leading "/" and checks whether the resulting {@code path} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 * 		efforts are made to normalize it.
	 * @return {@code true} if {@code path} exists as a key and has no trailing slash, {@code false} otherwise
	 */
	@Override
	public boolean isFile(final String normalPath) {

		return !normalPath.endsWith("/") && keyExists(removeLeadingSlash(normalPath));
	}

	@Override
	public LockedChannel lockForReading(final String normalPath) throws IOException {

		return new S3ObjectChannel(removeLeadingSlash(normalPath), true);
	}

	@Override
	public LockedChannel lockForWriting(final String normalPath) throws IOException {

		return new S3ObjectChannel(removeLeadingSlash(normalPath), false);
	}

	/**
	 * List all 'directory'-like children of a path.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 * 		efforts are made to normalize it.
	 * @return
	 * @throws IOException
	 */
	@Override
	public String[] listDirectories(final String normalPath) {

		return list(normalPath, true);
	}

	private String[] list(final String normalPath, final boolean onlyDirectories) {

		if (!exists(normalPath)) {
			throw new N5Exception.N5IOException(normalPath + " is not a valid group");
		}

		final List<String> subGroups = new ArrayList<>();
		final String prefix = removeLeadingSlash(addTrailingSlash(normalPath));
		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName(bucketName)
				.withPrefix(prefix)
				.withDelimiter("/");
		ListObjectsV2Result objectsListing;
		do {
			objectsListing = s3.listObjectsV2(listObjectsRequest);
			for (final String commonPrefix : objectsListing.getCommonPrefixes()) {
				if (!onlyDirectories || commonPrefix.endsWith("/")) {
					final String relativePath = relativize(commonPrefix, prefix);
					// TODO: N5AmazonS3Reader#list used replaceBackSlashes(relativePath) here. Is this necessary?
					if (!relativePath.isEmpty())
						subGroups.add(relativePath);
				}
			}
			listObjectsRequest.setContinuationToken(objectsListing.getNextContinuationToken());
		} while (objectsListing.isTruncated());
		return subGroups.toArray(new String[subGroups.size()]);
	}

	@Override
	public String[] list(final String normalPath) throws IOException {

		return list(normalPath, false);
	}

	@Override
	public void createDirectories(final String normalPath) throws IOException {

		String path = "";
		for (final String component : components(removeLeadingSlash(normalPath))) {
			path = addTrailingSlash(compose(path, component));
			if (path.equals("/")) {
				continue;
			}
			final ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(0);
			s3.putObject(
					bucketName,
					path,
					new ByteArrayInputStream(new byte[0]),
					metadata);
		}
	}

	@Override
	public void delete(final String normalPath) throws IOException {

		// remove bucket when deleting "/"
		if (normalPath.equals(normalize("/"))) {
			s3.deleteBucket(bucketName);
			return;
		}

		final String path = removeLeadingSlash(normalPath);

		if (!path.endsWith("/")) {
			s3.deleteObjects(new DeleteObjectsRequest(bucketName)
					.withKeys(new String[]{path}));
		}

		final String prefix = addTrailingSlash(path);
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
	}

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
		public int read(final byte[] b, final int off, final int len) throws IOException {

			return in.read(b, off, len);
		}

		@Override
		public boolean markSupported() {

			return in.markSupported();
		}

		@Override
		public void mark(final int readlimit) {

			in.mark(readlimit);
		}

		@Override
		public void reset() throws IOException {

			in.reset();
		}

		@Override
		public int available() throws IOException {

			return in.available();
		}

		@Override
		public long skip(final long n) throws IOException {

			return in.skip(n);
		}

		@Override
		public void close() throws IOException {

			if (!closed) {
				do {
					in.skip(in.available());
				} while (read() != -1);
				in.close();
				closed = true;
			}
		}
	}

	private class S3ObjectChannel implements LockedChannel {

		protected final String path;
		final boolean readOnly;
		private final ArrayList<Closeable> resources = new ArrayList<>();

		protected S3ObjectChannel(final String path, final boolean readOnly) throws IOException {

			this.path = path;
			this.readOnly = readOnly;
		}

		private void checkWritable() {

			if (readOnly) {
				throw new NonReadableChannelException();
			}
		}

		@Override
		public InputStream newInputStream() throws IOException {

			final S3ObjectInputStream in = s3.getObject(bucketName, path).getObjectContent();
			final S3ObjectInputStreamDrain s3in = new S3ObjectInputStreamDrain(in);
			synchronized (resources) {
				resources.add(s3in);
			}
			return s3in;
		}

		@Override
		public Reader newReader() throws IOException {

			final InputStreamReader reader = new InputStreamReader(newInputStream(), StandardCharsets.UTF_8);
			synchronized (resources) {
				resources.add(reader);
			}
			return reader;
		}

		@Override
		public OutputStream newOutputStream() throws IOException {

			checkWritable();
			return new S3OutputStream();
		}

		@Override
		public Writer newWriter() throws IOException {

			checkWritable();
			final OutputStreamWriter writer = new OutputStreamWriter(newOutputStream(), StandardCharsets.UTF_8);
			synchronized (resources) {
				resources.add(writer);
			}
			return writer;
		}

		@Override
		public void close() throws IOException {

			synchronized (resources) {
				for (final Closeable resource : resources)
					resource.close();
				resources.clear();
			}
		}

		final class S3OutputStream extends OutputStream {
			private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

			private boolean closed = false;

			@Override
			public void write(final byte[] b, final int off, final int len) throws IOException {

				buf.write(b, off, len);
			}

			@Override
			public void write(final int b) throws IOException {

				buf.write(b);
			}

			@Override
			public synchronized void close() throws IOException {

				if (!closed) {
					closed = true;
					final byte[] bytes = buf.toByteArray();
					final ObjectMetadata objectMetadata = new ObjectMetadata();
					objectMetadata.setContentLength(bytes.length);
					try (final InputStream data = new ByteArrayInputStream(bytes)) {
						s3.putObject(bucketName, path, data, objectMetadata);
					}
					buf.close();
				}
			}
		}
	}
}
