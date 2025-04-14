package org.janelia.saalfeldlab.n5.s3;

import com.amazonaws.services.s3.AmazonS3;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.kva.AbstractKeyValueAccessTest;
import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;

import java.net.URI;

public class AmazonS3KeyValueAccessTest extends AbstractKeyValueAccessTest {


	@Override protected KeyValueAccess newKeyValueAccess(URI root) {

		final AmazonS3 s3 = MockS3Factory.getOrCreateS3();
		return new AmazonS3KeyValueAccess(s3, root, true, true);
	}

	@Override protected URI tempUri() {

		final String tmpBucketAndContainer = N5AmazonS3Tests.tempBucketName() + N5AmazonS3Tests.tempContainerPath();
		return N5URI.getAsUri("s3://" + tmpBucketAndContainer);
	}
}
