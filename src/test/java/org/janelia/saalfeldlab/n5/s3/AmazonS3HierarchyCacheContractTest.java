package org.janelia.saalfeldlab.n5.s3;

import java.net.URI;
import java.util.ArrayList;
import org.janelia.saalfeldlab.n5.HierarchyStore;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueRoot;
import org.janelia.saalfeldlab.n5.KeyValueRootHierarchyStore;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.cache.AbstractHierarchyCacheContractTest;
import org.janelia.saalfeldlab.n5.s3.backend.BackendS3Factory;
import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;
import software.amazon.awssdk.services.s3.S3Client;
import org.junit.After;


public class AmazonS3HierarchyCacheContractTest extends AbstractHierarchyCacheContractTest {

	@Override
	protected HierarchyStore createStore() {

		return new KeyValueRootHierarchyStore(newKeyValueRoot());
	}

	protected S3Client getS3() {

		// TODO
		return MockS3Factory.getOrCreateS3();
//		return BackendS3Factory.getOrCreateS3();
	}

	private static URI tempUri() {

		final String tmpBucketAndContainer = N5AmazonS3Tests.tempBucketName() + N5AmazonS3Tests.tempContainerPath();
		return N5URI.getAsUri("s3://" + tmpBucketAndContainer);
	}

	private final ArrayList<AmazonS3KeyValueRoot> kvrs = new ArrayList<>();

	@After
	public void after() {
		// clean up, deletes any buckets that were created
		kvrs.forEach( kvr -> kvr.delete("/"));
		kvrs.clear();
	}

	private KeyValueRoot newKeyValueRoot() {

		final String bucketName = N5AmazonS3Tests.tempBucketName();
		final String root = N5AmazonS3Tests.tempContainerPath();
		final AmazonS3KeyValueRoot kvr = new AmazonS3KeyValueRoot(getS3(), bucketName, root, true);
		kvrs.add(kvr);
		return kvr;
	}
}
