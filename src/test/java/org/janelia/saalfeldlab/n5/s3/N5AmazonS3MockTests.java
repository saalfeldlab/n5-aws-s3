package org.janelia.saalfeldlab.n5.s3;

import com.amazonaws.services.s3.AmazonS3;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Tests.UseCache;
import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

public class N5AmazonS3MockTests extends N5AmazonS3Tests {

	@Override
	protected AmazonS3 getS3() {

		return MockS3Factory.getOrCreateS3();
	}
}
