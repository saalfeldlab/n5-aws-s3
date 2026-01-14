package org.janelia.saalfeldlab.n5.s3;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.kva.AbstractKeyValueAccessTest;
import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;
import org.junit.After;

import java.net.URI;
import java.util.ArrayList;

public class AmazonS3KeyValueAccessTest extends AbstractKeyValueAccessTest {
	
	private ArrayList<AmazonS3KeyValueAccess> kvas;

	@Override
	protected KeyValueAccess newKeyValueAccess(URI root) {

		if (kvas == null)
			kvas = new ArrayList<>();

		final AmazonS3KeyValueAccess kva = new AmazonS3KeyValueAccess(MockS3Factory.getOrCreateS3(), root, true);
		kvas.add(kva);
		return kva;
	}

	@Override protected URI tempUri() {

		final String tmpBucketAndContainer = N5AmazonS3Tests.tempBucketName() + N5AmazonS3Tests.tempContainerPath();
		return N5URI.getAsUri("s3://" + tmpBucketAndContainer);
	}

	@After
	public void after() {
		// clean up, deletes any buckets that were created
		kvas.forEach( kva -> {
			kva.delete("/");
		});

	}
}
