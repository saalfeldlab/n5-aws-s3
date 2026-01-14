package org.janelia.saalfeldlab.n5.s3;

import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;

import org.junit.Ignore;
import org.junit.Test;

import software.amazon.awssdk.services.s3.S3Client;

public class N5AmazonS3MockTests extends N5AmazonS3Tests {

	@Override
	protected S3Client getS3() {

		return MockS3Factory.getOrCreateS3();
	}

	@Test
	@Ignore("Erroneous NoSuchBucket Skipped for Mock Tests")
	@Override
	public void testErroneousNoSuchBucketFailure() {}
}
