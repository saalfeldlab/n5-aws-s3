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

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class AmazonS3KeyValueAccess implements KeyValueAccess {

	private final S3Client s3;
	private final URI containerURI;
	private final String bucketName;
	private final S3IoPolicy ioPolicy;

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

		this.ioPolicy = setIoPolicy();

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

	private S3IoPolicy setIoPolicy() {

		String ioPolicy = System.getProperty("n5.ioPolicy");
		if (ioPolicy == null)
			return new S3IoPolicy.EtagMatch(s3, bucketName);

		switch (ioPolicy) {
			case "unsafe":
			case "atomicFallbackUnsafe": // For S3, this is equivalent ot just Unsafe
				return new S3IoPolicy.Unsafe(s3, bucketName);
			case "atomic":
			default:
				return new S3IoPolicy.EtagMatch(s3, bucketName);
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
			headObjectRequest(s3, bucketName, key, null);
			return true;
		} catch( N5NoSuchKeyException | NoSuchKeyException e ) {
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
	static String addTrailingSlash(final String path) {

		return path.endsWith("/") ? path : path + "/";
	}

	/**
	 * When absolute paths are passed (e.g. /group/data), AWS S3 service creates an additional root folder with an empty name.
	 * This method removes the root slash symbol and returns the corrected path.
	 *
	 * @param path the path
	 * @return the path without the leading slash
	 */
	static String removeLeadingSlash(final String path) {

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
		return headObjectRequest(s3, bucketName, key, null).contentLength();
	}

	@Override
	public VolatileReadData createReadData(String normalPath) throws N5Exception.N5IOException {

		final String key = AmazonS3Utils.getS3Key(normalPath);
        try {
            return ioPolicy.read(key);
        } catch (IOException e) {
            throw new N5IOException(e);
        }
	}

	@Override
	public void write(final String normalPath, final ReadData data) throws N5IOException {

		final String key = AmazonS3Utils.getS3Key(normalPath);
		final String normalizedKey = removeLeadingSlash(key);

        try {
            ioPolicy.write(normalizedKey, data);
        } catch (IOException e) {
            throw new N5IOException(e);
        }
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
        try {
            ioPolicy.delete(key);
        } catch (IOException e) {
            throw new N5IOException(e);
        }
	}

	static HeadObjectResponse headObjectRequest(final S3Client s3, final String bucketName, final String key, final String matchEtag) {
		HeadObjectRequest.Builder requestBuilder = HeadObjectRequest.builder()
				.bucket(bucketName)
				.key(key);

		if (matchEtag != null)
			requestBuilder.ifMatch(matchEtag);

		final HeadObjectRequest request = requestBuilder.build();

		return rethrowS3Exceptions(() -> s3.headObject(request));
	}

	static <T> T rethrowS3Exceptions(Supplier<T> action) {
		try {
			return action.get();
		} catch (final NoSuchKeyException e) {
			throw new N5Exception.N5NoSuchKeyException("No such key", e);
		} catch (final AwsServiceException e) {
			final int statusCode = e.awsErrorDetails().sdkHttpResponse().statusCode();
			if (statusCode == 404 || statusCode == 403)
				throw new N5Exception.N5NoSuchKeyException("No such key", e);
			if (statusCode == 412)
				throw new N5ConcurrentModificationException("eTag has changed since initial request", e);
			throw new N5Exception.N5IOException("S3 Exception", e);
		}
	}

	//TODO: Move to N5
	static class N5ConcurrentModificationException extends N5Exception {

		public N5ConcurrentModificationException(String message) {
			super(message);
		}

		public N5ConcurrentModificationException(String message, Throwable cause) {
			super(message, cause);
		}

		public N5ConcurrentModificationException(Throwable cause) {
			super(cause);
		}

	}

	@Override
	@Deprecated
	public LockedChannel lockForReading(final String normalPath) {

		throw new UnsupportedOperationException("Deprecated; use `createReadData`");
	}

	@Override
	@Deprecated
	public LockedChannel lockForWriting(final String normalPath) {

		return new LockedChannel() {

			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			@Override
			public Reader newReader() throws N5IOException {
				return null;
			}

			@Override
			public InputStream newInputStream() throws N5IOException {
				return null;
			}

			@Override
			public Writer newWriter() throws N5IOException {
				return new BufferedWriter(new OutputStreamWriter(baos));
			}

			@Override
			public OutputStream newOutputStream() throws N5IOException {
				return baos;
			}

			@Override
			public void close() throws IOException {
				ReadData readData = ReadData.from(baos.toByteArray());
				write(normalPath, readData);
			}
		};
	}

	static class S3LazyRead implements LazyRead {

		private final String s3Key;
		private final boolean verifyEtag;
		private final S3Client s3;
		private final String bucketName;
		private String eTag = null;


		S3LazyRead(final S3Client s3, final String bucketName, final String s3Key, final boolean verifyEtag) {
			this.s3 = s3;
			this.bucketName = bucketName;
			this.s3Key = s3Key;
			this.verifyEtag = verifyEtag;
		}

		private String getEtag(final String s3Key) {
			HeadObjectRequest headRequest = HeadObjectRequest.builder()
					.bucket(bucketName)
					.key(s3Key)
					.build();

			return s3.headObject(headRequest).eTag();
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

			if (verifyEtag) {
				if (eTag == null)
					eTag = getEtag(s3Key);
				requestBuilder.ifMatch(eTag);
			}

			return requestBuilder.build();
		}

		@Override public ReadData materialize(long offset, long length) throws N5Exception.N5IOException {

			final ResponseBytes<GetObjectResponse> response = rethrowS3Exceptions(() -> {
				final GetObjectRequest request = createObjectRequest(s3Key, offset, length);
				return s3.getObject(request, ResponseTransformer.toBytes());
			});
			return ReadData.from(response.asByteArray());
		}

		@Override public long size() throws N5Exception.N5IOException {

			if (!verifyEtag)
				eTag = null;

			final HeadObjectResponse response = headObjectRequest(s3, bucketName, s3Key, eTag);

			if (eTag == null && verifyEtag)
				eTag = response.eTag();

			return response.contentLength();
		}

		@Override
		public void close() {
			eTag = null;
		}
	}
}
