package org.janelia.saalfeldlab.n5.s3.backend;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.security.SecureRandom;

import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import org.janelia.saalfeldlab.n5.s3.AmazonS3KeyValueAccess;
import org.janelia.saalfeldlab.n5.s3.S3IoPolicy;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import software.amazon.awssdk.services.s3.S3Client;


public class BackendIoPolicyTests {

	private static final SecureRandom random = new SecureRandom();
	static final String UNSAFE_KEY = "unsafe/obj";
	static final String ETAG_KEY = "etag/obj";

	protected static S3Client s3;
	protected static AmazonS3KeyValueAccess kva;
	protected static String bucketName;

	@BeforeClass
	public static void setup() {

		s3 = BackendS3Factory.getOrCreateS3();
		bucketName = "n5-test-" + Long.toUnsignedString(random.nextLong());
		kva = new AmazonS3KeyValueAccess(s3, URI.create("s3://" + bucketName), true);
	}

	@AfterClass
	public static void teardown() {

		if (kva != null) {
			kva.delete("/"); // delete the bucket and everything in it
		}
	}

	@Test
	public void testUnsafe() throws IOException {

		final S3IoPolicy.Unsafe policy = new S3IoPolicy.Unsafe(s3, bucketName);
		final byte[] data = {0, 1, 2, 3, 4};
		final byte[] data2 = {5, 6, 7};

		// write and full read roundtrip
		policy.write(UNSAFE_KEY, ReadData.from(data));

		try (VolatileReadData result = policy.read(UNSAFE_KEY)) {
			assertArrayEquals(data, result.allBytes());
		}

		// concurrent modification: 
		// overwrite the blob, then attempt to materialize, shoud succeed
		try (VolatileReadData vrd = policy.read(UNSAFE_KEY)) {

			// should not store the generation
			vrd.requireLength();

			// overwrite to new generation
			policy.write(UNSAFE_KEY, ReadData.from(data2));

			// ensure fetching the data gets the new data and does not error
			assertArrayEquals(data2, vrd.allBytes());
		}
	}

	@Test
	public void testEtagMatch() throws IOException {

		final S3IoPolicy.EtagMatch policy = new S3IoPolicy.EtagMatch(s3, bucketName);
		final byte[] data1 = {0, 1, 2, 3, 4};
		final byte[] data2 = {5, 6, 7};

		// write and full read roundtrip
		policy.write(ETAG_KEY, ReadData.from(data1));
		try (VolatileReadData result = policy.read(ETAG_KEY)) {
			assertArrayEquals(data1, result.allBytes());
		}

		// after close(), a new read sees updated content
		policy.write(ETAG_KEY, ReadData.from(data2));
		try (VolatileReadData result = policy.read(ETAG_KEY)) {
			assertArrayEquals(data2, result.allBytes());
		}

		// concurrent modification: pin the generation via requireLength(),
		// overwrite the blob, then attempt to materialize
		policy.write(ETAG_KEY, ReadData.from(data1));
		try (VolatileReadData vrd = policy.read(ETAG_KEY)) {

			// sets the generation of vrd
			vrd.requireLength();

			// overwrite to new generation
			policy.write(ETAG_KEY, ReadData.from(data2));

			// ensure fetching the data
			assertThrows(N5Exception.N5ConcurrentModificationException.class, vrd::allBytes);
		}
	}

}
