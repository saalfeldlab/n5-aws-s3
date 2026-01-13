package org.janelia.saalfeldlab.n5.s3;

import java.net.URI;
import java.nio.file.Path;

import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;
import org.janelia.saalfeldlab.n5.s3.mock.RunnerWithMinioServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.junit.Ignore;
import org.junit.Test;

import software.amazon.awssdk.services.s3.S3Client;

public class N5AmazonS3MockTests extends N5AmazonS3Tests {

	public static Path minioServerDirectory;

	public static URI minioUri;

	public static RunnerWithMinioServer runner;

	static {
		/* Should be no Erroneous Backend Failures with Mock Backend */
		skipErroneousBackendFailures = false;
	}

	@BeforeClass
	public static void startServer() throws Exception {

		runner = new RunnerWithMinioServer(N5AmazonS3MockTests.class);
		runner.startMinioServer();

		minioServerDirectory = runner.minioServerDirectory;
		minioUri = runner.minioUri;
	}

	@AfterClass
	public static void stopServer() {

		runner.stopMinioServer();
	}

	@Override
	protected S3Client getS3() {

		return MockS3Factory.getOrCreateS3();
	}

	@Test
	@Ignore("Erroneous NoSuchBucket Skipped for Mock Tests")
	@Override
	public void testErroneousNoSuchBucketFailure() {}
}
