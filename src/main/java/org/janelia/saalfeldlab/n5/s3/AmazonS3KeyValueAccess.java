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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccessReadData;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class AmazonS3KeyValueAccess implements KeyValueAccess {

	private final S3Client s3;
	private final URI containerURI;
	private final String bucketName;

	private final boolean createBucket;
	private Boolean bucketCheckedAndExists = null;

	/**
	 * Opens an {@link AmazonS3KeyValueAccess} using an {@link S3Client} client and a given bucket name.
	 * <p>
	 * If the bucket does not exist and {@code createBucket==true}, the bucket will be created.
	 * If the bucket does not exist and {@code createBucket==false}, the bucket will not be
	 * created and all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3           the s3 instance
	 * @param containerURI the URI that points to the n5 container root.
	 * @param createBucket whether {@code bucketName} should be created if it doesn't exist
	 * @throws N5Exception.N5IOException if the access could not be created
	 * @deprecated containerURI must be valid URI, call constructor with URI instead of String {@link AmazonS3KeyValueAccess#AmazonS3KeyValueAccess(S3Client, URI, boolean)}
	 */
	@Deprecated
	public AmazonS3KeyValueAccess(final S3Client s3, String containerURI, final boolean createBucket) throws N5Exception.N5IOException {

		this(s3, N5URI.getAsUri(containerURI), createBucket);
	}

	/**
	 * Opens an {@link AmazonS3KeyValueAccess} using an {@link S3Client} client and a given bucket name.
	 * <p>
	 * If the bucket does not exist and {@code createBucket==true}, the bucket will be created.
	 * If the bucket does not exist and {@code createBucket==false}, the bucket will not be
	 * created and all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3                   the s3 instance
	 * @param containerURI         the URI that points to the n5 container root.
	 * @param createBucket         whether {@code bucketName} should be created if it doesn't exist
	 * @throws N5Exception.N5IOException if the access could not be created
	 */
	public AmazonS3KeyValueAccess(final S3Client s3, final URI containerURI, final boolean createBucket) throws N5Exception.N5IOException {

		this.s3 = s3;
		this.containerURI = containerURI;

		this.bucketName = AmazonS3Utils.getS3Bucket(containerURI);
		this.createBucket = createBucket;

		ensureS3EndpointIsReachable();

		if (!bucketExists()) {
			if (createBucket) {
				s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
				bucketCheckedAndExists = true;
			} else {
				throw new N5Exception.N5IOException(
						"Bucket " + bucketName + " does not exist, and you told me not to create one.");
			}
		}
	}

	private void ensureS3EndpointIsReachable() throws N5Exception.N5IOException {
		try {
			s3.listBuckets();
		} catch (Exception e) {
			throw new N5Exception.N5IOException("Could not reach S3 endpoint", e);
		}
	}

	private boolean bucketExists() {

		return bucketCheckedAndExists = bucketCheckedAndExists != null
				? bucketCheckedAndExists
				: AmazonS3Utils.bucketExists(s3, bucketName);
	}

	private void createBucket() {

		if (!createBucket)
			throw new N5Exception("Create Bucket Not Allowed");

		if (bucketExists())
			return;

		try {
			s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
			bucketCheckedAndExists = true;
		} catch (Exception e) {
			throw new N5Exception("Could not create bucket " + bucketName, e);
		}

	}

	private void deleteBucket() {

		if (!createBucket)
			throw new N5Exception("Delete Bucket Not Allowed");

		// TODO consider not checking existence of bucket
		if (!bucketExists())
			return;

		try {
			AmazonS3Utils.deleteBucket(s3, bucketName);
			bucketCheckedAndExists = false;
		} catch (S3Exception e) {
			throw new N5Exception("Could not delete bucket " + bucketName, e);
		}
	}

	@Override
	public String[] components(final String path) {


		/* If the path is a valid URI with a scheme then use it to get the key. Otherwise,
		* use the path directly, assuming it's a path only */
		String key = path;
		try {
			final URI uri = N5URI.getAsUri(path);
			final String scheme = uri.getScheme();
			if (scheme != null && !scheme.isEmpty())
				key = AmazonS3Utils.getS3Key(uri);
		} catch (Throwable ignore) {}

		return KeyValueAccess.super.components(key);
	}

	@Override
	public String relativize(final String path, final String base) {

		try {
			/* Must pass absolute path to `uri`. if it already is, this is redundant, and has no impact on the result.
			* 	It's not true that the inputs are always referencing absolute paths, but it doesn't matter in this
			* 	case, since we only care about the relative portion of `path` to `base`, so the result always
			* 	ignores the absolute prefix anyway. */
			final URI baseAsUri = uri("/" + base);
			final URI pathAsUri = uri("/" + path);
			final URI relativeUri = baseAsUri.relativize(pathAsUri);
			return relativeUri.getPath();
		} catch (final URISyntaxException e) {
			throw new N5Exception("Cannot relativize path (" + path + ") with base (" + base + ")", e);
		}
	}

	@Override
	public String normalize(final String path) {

		return N5URI.normalizeGroupPath(path);
	}

	/**
	 * Create a URI that is the result of resolving the `normalPath` against the {@link #containerURI}.
	 * NOTE: {@link URI#resolve(URI)} always removes the last member of the receiver URIs path.
	 * That is undesirable behavior here, as we want to potentially keep the containerURI's
	 * full path, and just append `normalPath`. However, it's more complicated, as `normalPath`
	 * can also contain leading overlap with the trailing members of `containerURI.getPath()`.
	 * To properly resolve the two paths, we generate {@link Path}s from the results of {@link URI#getPath()}
	 * and use {@link Path#resolve(Path)}, which results in a guaranteed absolute path, with the
	 * desired path resolution behavior. That then is used to construct a new {@link URI}.
	 * Any query or fragment portions are ignored. Scheme and Authority are always
	 * inherited from {@link #containerURI}.
	 *
	 * @param normalPath EITHER a normalized path, or a valid URI
	 * @return the URI generated from resolving normalPath against containerURI
	 * @throws URISyntaxException if the given normal path is not a valid URI
	 */
	@Override
	public URI uri(final String normalPath) throws URISyntaxException {

		return KeyValueAccess.super.uri(compose(containerURI, normalPath));
	}

	/**
	 * Test whether the {@code normalPath} exists.
	 * <p>
	 * Removes leading slash from {@code normalPath}, and then checks whether
	 * either {@code path} or {@code path + "/"} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 *                   efforts are made to normalize it.
	 * @return {@code true} if {@code path} exists, {@code false} otherwise
	 */
	@Override
	public boolean exists(final String normalPath) {

		return isFile(normalPath) || isDirectory(normalPath);
	}

	private ListObjectsV2Response queryPrefix(final String prefix) {

		final ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
				.bucket(bucketName)
				.prefix(prefix)
				.maxKeys(1)
				.build();

		return s3.listObjectsV2(listObjectsV2Request);
	}

	/**
	 * Check existence of the given {@code key}.
	 * <p>
	 * Does not distinguish between an object not existing and not having permissions to
	 * access the object.
	 *
	 * @return {@code true} if {@code key} exists.
	 */
	private boolean keyExists(final String key) {

		try {
			// TODO needs testing.
			// the exception thrown may depend on permissions
			@SuppressWarnings("unused")
			HeadObjectResponse headObjectResponse = s3.headObject(
				HeadObjectRequest.builder().key(key).bucket(bucketName).build());

			return true;
		} catch( NoSuchKeyException e ) {
			return false;
		} catch (Throwable e) {
			throw new N5Exception(e);
		}
	}

	/**
	 * Check existence of the given {@code prefix}.
	 *
	 * @return {@code true} if {@code prefix} exists.
	 */
	private boolean prefixExists(final String prefix) {

		try {
			final ListObjectsV2Response objectsListing = queryPrefix(prefix);
			return objectsListing.keyCount() > 0;
		} catch (NoSuchBucketException e) {
		}
		return false;
	}

	/**
	 * When listing children objects for a group, must append a delimiter to the path (e.g. group/data/).
	 * This is necessary for not including wrong objects in the filtered set
	 * (e.g. group/data-2/attributes.json when group/data is passed without the last slash).
	 *
	 * @param path the path
	 * @return the path with a trailing slash
	 */
	private static String addTrailingSlash(final String path) {

		return path.endsWith("/") ? path : path + "/";
	}

	/**
	 * When absolute paths are passed (e.g. /group/data), AWS S3 service creates an additional root folder with an empty name.
	 * This method removes the root slash symbol and returns the corrected path.
	 *
	 * @param path the path
	 * @return the path without the leading slash
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
	 *                   efforts are made to normalize it.
	 * @return {@code true} if {@code path} (with trailing "/") exists as a key, {@code false} otherwise
	 */
	@Override
	public boolean isDirectory(final String normalPath) {

		final String s3Key = AmazonS3Utils.getS3Key(N5URI.getAsUri(normalPath));
		final String key = removeLeadingSlash(addTrailingSlash(s3Key));
		if (key.equals(normalize("/"))) {
			return bucketExists();
		}
		return prefixExists(key);
	}

	/**
	 * Test whether the path is a file.
	 * <p>
	 * Checks whether {@code normalPath} has no trailing "/", then removes
	 * leading "/" and checks whether the resulting {@code path} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 *                   efforts are made to normalize it.
	 * @return {@code true} if {@code path} exists as a key and has no trailing slash, {@code false} otherwise
	 */
	@Override
	public boolean isFile(final String normalPath) {

		final String key = AmazonS3Utils.getS3Key(normalPath);
		return !key.endsWith("/") && keyExists(removeLeadingSlash(key));
	}

	@Override
	public long size(String normalPath) throws N5NoSuchKeyException {

		final String key = removeLeadingSlash(AmazonS3Utils.getS3Key(normalPath));

		final HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
				.bucket(bucketName)
				.key(key)
				.build();

		final HeadObjectResponse response = rethrowS3Exceptions(() -> s3.headObject(headObjectRequest));
		return response.contentLength();
	}

	@Override public ReadData createReadData(String normalPath) throws N5Exception.N5IOException {

		return new KeyValueAccessReadData(new S3LazyRead(normalPath));
	}

	private <T> T rethrowS3Exceptions(Supplier<T> action) {
		try {
			return action.get();
		} catch (final NoSuchKeyException e) {
				throw new N5Exception.N5NoSuchKeyException("No such key", e);
		} catch (final AwsServiceException e) {
			final int statusCode = e.awsErrorDetails().sdkHttpResponse().statusCode();
			if (statusCode == 404 || statusCode == 403)
				throw new N5Exception.N5NoSuchKeyException("No such key", e);
			throw new N5Exception.N5IOException("S3 Exception", e);
		}
	}

	private class S3LazyRead implements LazyRead {

		private final String key;

		S3LazyRead(final String normalizedKey) {
			this.key = normalizedKey;
		}

		private GetObjectRequest createObjectRequest(final String s3Key, long offset, long length) {

			final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
					.key(s3Key)
					.bucket(bucketName);

			// Only add range header if we're doing a partial read
			if (offset > 0 || length > 0) {
				// HTTP Range header format: "bytes=start-end"
				// If length is 0 or negative, read from offset to end of file
				final String range = length > 0
						? String.format("bytes=%d-%d", offset, offset + length - 1)
						: String.format("bytes=%d-", offset);
				requestBuilder.range(range);
			}

			return requestBuilder.build();
		}

		@Override public ReadData materialize(long offset, long length) throws N5Exception.N5IOException {

			final String s3Key = AmazonS3Utils.getS3Key(key);
			final GetObjectRequest request = createObjectRequest(s3Key, offset, length);
			final ResponseBytes<GetObjectResponse> response = rethrowS3Exceptions(() -> s3.getObject(request, ResponseTransformer.toBytes()));
			return ReadData.from(response.asByteArray());
		}

		@Override public long size() throws N5Exception.N5IOException {

			return AmazonS3KeyValueAccess.this.size(key);
		}
	}

	@Override
	public LockedChannel lockForReading(final String normalPath) {

		final String key = AmazonS3Utils.getS3Key(normalPath);
		return new S3ObjectChannel(key, true);
	}

	@Override
	public LockedChannel lockForWriting(final String normalPath) {

		final String key = AmazonS3Utils.getS3Key(normalPath);
		return new S3ObjectChannel(removeLeadingSlash(key), false);
	}

	@Override
	public String[] listDirectories(final String normalPath) {

		final String[] directories = list(normalPath, true);
		for (int i = 0; i < directories.length; i++) {
			/* list can return `/` suffix if a directory, but this is not enforced.
			* To be consistent, `listDirectories` should normalize (remove the trailing `/`)
			* since we don't need it to know that the path is a directory from this method.*/
			directories[i] = normalize(directories[i]);
		}
		return directories;
	}

	private String[] list(final String normalPath, final boolean onlyDirectories) {

		final String pathKey = AmazonS3Utils.getS3Key(normalPath);
		final List<String> subGroups = new ArrayList<>();
		final String prefix = removeLeadingSlash(addTrailingSlash(pathKey));

		final ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
				.bucket(bucketName)
				.prefix(prefix)
				.delimiter("/")
				.build();

		s3.listObjectsV2Paginator(listObjectsV2Request).commonPrefixes().forEach(p -> {
			if (!onlyDirectories || p.prefix().endsWith("/")) {
				final String relativePath = relativize(p.prefix(), prefix);
				if (!relativePath.isEmpty())
					subGroups.add(relativePath);
			}
		});

		if (subGroups.size() <= 0) {
			if(!isDirectory(normalPath))
				throw new N5Exception.N5IOException(normalPath + " is not a valid group");
		}

		return subGroups.toArray(new String[subGroups.size()]);
	}

	@Override
	public String[] list(final String normalPath) {

		return list(normalPath, false);
	}

	@Override
	public void createDirectories(final String normalPath) {

		if (!bucketExists() && createBucket) {
			createBucket();
		}

		String path = "";
		for (final String component : components(removeLeadingSlash(normalPath))) {
			final String composed = addTrailingSlash(compose(path, component));
			if (composed.equals("/"))
				continue;

			path = composed;
			final PutObjectRequest putOb = PutObjectRequest.builder()
					.bucket(bucketName)
					.key(path)
					.contentLength((long)0)
					.build();

			s3.putObject(putOb, RequestBody.fromBytes(new byte[0]));
		}
	}

	@Override
	public void delete(final String normalPath) {

		if (!bucketExists())
			return;

		// remove bucket when deleting "/"
		if (AmazonS3Utils.getS3Key(normalPath).equals(normalize("/"))) {
			deleteBucket(); // also deletes all contents
			return;
		}

		final String key = removeLeadingSlash(AmazonS3Utils.getS3Key(normalPath));
		if (!key.endsWith("/")) {

			final DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
				.bucket(bucketName)
				.key(key)
				.build();

			try {
				s3.deleteObject(deleteRequest);
			} catch (S3Exception e) {
			}
		}

		final String prefix = addTrailingSlash(key);

		ListObjectsV2Request listObjectsRequest;
		ListObjectsV2Response objectsListing;
		listObjectsRequest = ListObjectsV2Request.builder()
				.bucket(bucketName)
				.prefix(prefix)
				.build();

		do {
			objectsListing = s3.listObjectsV2(listObjectsRequest);
			final List<ObjectIdentifier> objectsToDelete = objectsListing.contents().stream().map( x -> {
				return ObjectIdentifier.builder().key(x.key()).build();
			}).collect(Collectors.toList());

			if (!objectsToDelete.isEmpty()) {
				final DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
					.bucket(bucketName)
					.delete( Delete.builder().objects(objectsToDelete).build())
					.build();

				s3.deleteObjects(deleteRequest);
			}

			// TODO what about continuation token?

		} while (objectsListing.isTruncated());
	}

	private class S3ObjectChannel implements LockedChannel {

		protected final String path;
		final boolean readOnly;
		private final ArrayList<Closeable> resources = new ArrayList<>();

		protected S3ObjectChannel(final String path, final boolean readOnly) {

			this.path = path;
			this.readOnly = readOnly;
		}

		private void checkWritable() {

			if (readOnly) {
				throw new NonReadableChannelException();
			}
		}

		@Override
		public InputStream newInputStream() {

			try {
				final GetObjectRequest objectRequest = GetObjectRequest.builder().key(path).bucket(bucketName).build();
				// TODO consider using ResponseTransformer.toBytes
	            return s3.getObject(objectRequest, ResponseTransformer.toInputStream());
			} catch (final S3Exception e) {
				// TODO figure out how to determine if the error is because the key does not exist
				if (e.statusCode() == 404 || e.statusCode() == 403)
					throw new N5Exception.N5NoSuchKeyException("No such key", e);
				else
					throw new N5Exception.N5IOException(e);
			}
		}

		@Override
		public Reader newReader() {

			final InputStreamReader reader = new InputStreamReader(newInputStream(), StandardCharsets.UTF_8);
			synchronized (resources) {
				resources.add(reader);
			}
			return reader;
		}

		@Override
		public OutputStream newOutputStream() {

			checkWritable();
			final S3OutputStream s3Out = new S3OutputStream();
			synchronized (resources) {
				resources.add(s3Out);
			}
			return s3Out;
		}

		@Override
		public Writer newWriter() {

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
			public void write(final byte[] b, final int off, final int len) {

				buf.write(b, off, len);
			}

			@Override
			public void write(final int b) {

				buf.write(b);
			}

			@Override
			public synchronized void close() throws IOException {

				if (!closed) {
					closed = true;
		            PutObjectRequest putOb = PutObjectRequest.builder()
		                .bucket(bucketName)
		                .key(path)
		                .build();

					try {
						s3.putObject(putOb, RequestBody.fromBytes(buf.toByteArray()));
					} catch (S3Exception e) {
						e.printStackTrace();
					}
					buf.close();
				}
			}
		}
	}


}
