package org.janelia.saalfeldlab.n5.s3;

import com.amazonaws.services.s3.AmazonS3;
import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

public class N5AmazonS3MockTests extends N5AmazonS3Tests {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {

		return Arrays.asList(new Object[][]{
				{"mock s3, container at generated path", null, false},
				{"mock s3, container at generated path , cache attributes", null, true},
				{"mock s3, container at root", "/", false},
				{"mock s3, container at root with , cache attributes", "/", true}
		});
	}

	@Override
	protected AmazonS3 getS3() {

		return MockS3Factory.getOrCreateS3();
	}
}
