/**
 * Copyright (c) 2017--2021, Saalfeld lab
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.BeforeClass;
import org.junit.Test;


/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class N5S3PathTest {

	private static final String[][] testPaths = new String[][] {
		{"/test/test1"},
		{"/", "test"},
		{"test/test1"},
		{"/"},
		{""}};

	private static final boolean[] isAbsolute = new boolean[] {
			true,
			true,
			false,
			true,
			false};

	private static final String[] testFilenames = new String[] {
		"test1",
		"test",
		"test1",
		"",
		null};


	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {}

	@Test
	public void testIsAbsolute() {

		for (int i = 0; i < testPaths.length; ++i)
			assertEquals(isAbsolute[i], new N5S3Path(null, testPaths[i]).isAbsolute());
	}

	@Test
	public void testGetRoot() {

		assertNotNull(new N5S3Path(null, "/test/test1").getRoot());
		assertNotNull(new N5S3Path(null, "/", "test").getRoot());
		assertNull(new N5S3Path(null, "test/test1").getRoot());
		assertNull(new N5S3Path(null, "").getRoot());
	}

	@Test
	public void testGetFileName() {

		assertEquals(new N5S3Path(null, "/test/test1").getFileName().toString(), "test1");
		assertEquals(new N5S3Path(null, "/", "test").getFileName().toString(), "test");
		assertEquals(new N5S3Path(null, "test/test1").getFileName().toString(), "test1");
		assertEquals(new N5S3Path(null, "/").getFileName().toString(), "");
		assertNull(new N5S3Path(null, "").getFileName());
	}



}
