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

import java.io.IOException;
import java.net.URI;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
//import org.janelia.saalfeldlab.n5.RootedKeyValueAccess;
//import org.janelia.saalfeldlab.n5.RootedURI;
//import org.janelia.saalfeldlab.n5.RootedURI.N5GroupPath;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.s3.S3RootedURI.N5FilePath;
import org.janelia.saalfeldlab.n5.s3.S3RootedURI.N5GroupPath;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class AmazonS3RootedKeyValueAccess
//implements RootedKeyValueAccess
{

	private final S3Client s3;
	private final URI root;
	private final String bucketName; // TODO: rename to "bucket"
	private S3IoPolicy ioPolicy;

	private final boolean createBucket;
	private Boolean bucketCheckedAndExists = null;

	/**
	 * Opens an {@link AmazonS3RootedKeyValueAccess} using an {@link S3Client}
	 * client and a given bucket name.
	 * <p>
	 * If the bucket does not exist and {@code createBucket==true}, the bucket
	 * will be created. If the bucket does not exist and {@code
	 * createBucket==false}, the bucket will not be created and all subsequent
	 * attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param s3
	 * 		the s3 instance
	 * @param root
	 * 		the URI that points to the n5 container root (relative to the bucket).
	 * @param createBucket
	 * 		whether {@code bucketName} should be created if it doesn't exist
	 *
	 * @throws N5IOException
	 * 		if the access could not be created
	 */
	public AmazonS3RootedKeyValueAccess(
			final S3Client s3,
			final String bucketName,
			final URI root,
			final boolean createBucket) throws N5IOException {

		this.s3 = s3;
		this.bucketName = bucketName;
		this.root = root; // TODO: We expect root to be a relative URI ending in "/" (or ""). Make sure of that.
		this.createBucket = createBucket;

		this.ioPolicy = new S3IoPolicy.Unsafe(s3, bucketName); // TODO: IoPolicy

		if (!bucketExists()) {
			if (createBucket) {
				s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
				bucketCheckedAndExists = true;
			} else {
				throw new N5IOException(
						"Bucket " + bucketName + " does not exist, and you told me not to create one.");
			}
		}
	}

	// ------------------------------------------------------------------------
	//
 	// -- from AmazonS3KeyValueAccess --
	//

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

	//
	// -- from AmazonS3KeyValueAccess --
	//
	// ------------------------------------------------------------------------

//	@Override
	public synchronized KeyValueAccess getKVA() {
		if (kva == null) {
			final String containerURI = URI.create("s3://" + bucketName + "/").resolve(root).toString();
			kva = new AmazonS3KeyValueAccess(s3, containerURI, createBucket);
		}
		return kva;
	}
	private AmazonS3KeyValueAccess kva;
//
//	@Override
	public URI root() {
		// TODO: Should this be root or the full "s3://..." URI ???
		//       ==> What is RootedKeyValueAccess.root() used for ???
		return root;
	}
//
//	@Override
//	public VolatileReadData createReadData(final URI normalPath) throws N5IOException {
//		throw new UnsupportedOperationException("TODO. not implemented yet");
//	}
//

	/**
	 * Test whether the path is a directory.
	 * <p>
	 * Appends trailing "/" to {@code normalPath} if there is none and then
	 * checks whether resulting {@code path} is a key.
	 *
	 * @param normalPath
	 * 		(relative to container root)
	 * 		is expected to be in normalized form, no further efforts are made to normalize it.
	 * @return {@code true} if {@code path} (with trailing "/") exists as a key, {@code false} otherwise
	 */
//	@Override
	public boolean isDirectory(final URI normalPath) {

		final URI uri = root.resolve(N5GroupPath.of(normalPath.getPath()).uri()); // TODO (N5Path): if we had isDirectory(N5GroupPath), we wouldn't have to do this
		final String prefix = uri.getPath();

		if (prefix.isEmpty()) {
			return bucketExists();
		}

		try {
			final ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
					.bucket(bucketName)
					.prefix(prefix)
					.maxKeys(1)
					.build();
			final Integer keyCount = s3.listObjectsV2(listObjectsV2Request).keyCount();
			/* keyCount should NEVER be null, and yet we have seen this in the wild... */
			return keyCount != null && keyCount > 0;
		} catch (NoSuchBucketException e) {
			return false;
		}
	}

//	@Override
//	public boolean isFile(final URI normalPath) {
//		throw new UnsupportedOperationException("TODO. not implemented yet");
//	}
//
//	@Override
//	public boolean exists(final URI normalPath) {
//		throw new UnsupportedOperationException("TODO. not implemented yet");
//	}
//
//	@Override
//	public long size(final URI normalPath) throws N5IOException {
//		throw new UnsupportedOperationException("TODO. not implemented yet");
//	}
//
//	@Override
	public void write(final URI normalPath, final ReadData data) throws N5IOException {

		final String key = N5FilePath.of(root.resolve(normalPath).getPath()).normalPath(); // TODO (N5Path): if we had write(N5GroupPath), we wouldn't have to do this
		try {
			ioPolicy.write(key, data);
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

//	@Override
//	public String[] listDirectories(final URI normalPath) throws N5IOException {
//		throw new UnsupportedOperationException("TODO. not implemented yet");
//	}
//
//	@Override
	public void createDirectories(final URI normalPath) throws N5IOException {

		if (!bucketExists() && createBucket) { // TODO: revisit bucket creation logic
			createBucket();
		}

		final N5GroupPath group = N5GroupPath.of(root.resolve(normalPath).getPath());
		if (group.normalPath().isEmpty())
			return;

		String key = "";
		for (final String child : group.components()) {
			key += child + "/";
			final PutObjectRequest putOb = PutObjectRequest.builder()
					.bucket(bucketName)
					.key(key)
					.contentLength((long)0)
					.build();
			s3.putObject(putOb, RequestBody.fromBytes(new byte[0]));
		}
	}
//
//	@Override
	public void delete(final URI normalPath) throws N5IOException {
		throw new UnsupportedOperationException("TODO. not implemented yet");
	}


}
