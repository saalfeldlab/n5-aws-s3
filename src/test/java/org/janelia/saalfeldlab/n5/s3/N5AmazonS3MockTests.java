package org.janelia.saalfeldlab.n5.s3;

import com.amazonaws.services.s3.AmazonS3;
import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;
import org.junit.Ignore;
import org.junit.Test;

public class N5AmazonS3MockTests extends N5AmazonS3Tests {


	static {
		/* Should be no Erroneous Backend Failures with Mock Backend */
		skipErroneousBackendFailures = false;
	}

	@Override
	protected AmazonS3 getS3() {

		return MockS3Factory.getOrCreateS3();
	}

	@Test
	@Ignore("Erroneous NoSuchBucket Skipped for Mock Tests")
	@Override
	public void testErroneousNoSuchBucketFailure() {
	}
}
