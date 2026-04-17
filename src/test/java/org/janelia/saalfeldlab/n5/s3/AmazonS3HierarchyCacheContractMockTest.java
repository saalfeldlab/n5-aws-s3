package org.janelia.saalfeldlab.n5.s3;

import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;
import software.amazon.awssdk.services.s3.S3Client;


public class AmazonS3HierarchyCacheContractMockTest extends AmazonS3HierarchyCacheContractTest {

	@Override
	protected S3Client getS3() {

		return MockS3Factory.getOrCreateS3();
	}
}
