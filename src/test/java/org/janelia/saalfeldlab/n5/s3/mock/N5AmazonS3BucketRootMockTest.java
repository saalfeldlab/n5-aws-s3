/*-
 * #%L
 * N5 AWS S3
 * %%
 * Copyright (C) 2017 - 2022, Saalfeld Lab
 * %%
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.s3.mock;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.s3.AbstractN5AmazonS3BucketRootTest;
import org.junit.Assert;

/**
 * Initiates testing of the Amazon Web Services S3-based N5 implementation using
 * S3 mock library.
 * The test N5 container is created at the root of the new temporary bucket.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5AmazonS3BucketRootMockTest extends AbstractN5AmazonS3BucketRootTest {

	public N5AmazonS3BucketRootMockTest() {

		super(MockS3Factory.getOrCreateS3());
	}

	/**
	 * TODO remove this redundant override after bumping to >= n5-2.5.2
	 *
	 * https://github.com/saalfeldlab/n5/commit/a1fcd2f6b3be7e1e10b08aaeba3912ffb5707d8c
	 */
	@Override
	public void testDeepList() {

		try {

			// clear container to start
			for (final String g : n5.list("/"))
				n5.remove(g);

			n5.createGroup(groupName);
			for (final String subGroup : subGroupNames)
				n5.createGroup(groupName + "/" + subGroup);

			final List<String> groupsList = Arrays.asList(n5.deepList("/"));
			for (final String subGroup : subGroupNames)
				Assert
						.assertTrue(
								"deepList contents",
								groupsList.contains(groupName.replaceFirst("/", "") + "/" + subGroup));

			for (final String subGroup : subGroupNames)
				Assert
						.assertTrue(
								"deepList contents",
								Arrays
										.asList(n5.deepList(""))
										.contains(groupName.replaceFirst("/", "") + "/" + subGroup));

			final DatasetAttributes datasetAttributes = new DatasetAttributes(
					dimensions,
					blockSize,
					DataType.UINT64,
					new RawCompression());
			final LongArrayDataBlock dataBlock = new LongArrayDataBlock(
					blockSize,
					new long[]{0, 0, 0},
					new long[blockNumElements]);
			n5.createDataset(datasetName, datasetAttributes);
			n5.writeBlock(datasetName, datasetAttributes, dataBlock);

			final List<String> datasetList = Arrays.asList(n5.deepList("/"));
			final N5Writer n5Writer = n5;

			for (final String subGroup : subGroupNames)
				Assert
						.assertTrue(
								"deepList contents",
								datasetList.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			Assert.assertTrue("deepList contents", datasetList.contains(datasetName.replaceFirst("/", "")));
			Assert.assertFalse("deepList stops at datasets", datasetList.contains(datasetName + "/0"));

			final List<String> datasetList2 = Arrays.asList(n5.deepList(""));
			for (final String subGroup : subGroupNames)
				Assert
						.assertTrue(
								"deepList contents",
								datasetList2.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			Assert.assertTrue("deepList contents", datasetList2.contains(datasetName.replaceFirst("/", "")));
			Assert.assertFalse("deepList stops at datasets", datasetList2.contains(datasetName + "/0"));

			final String prefix = "/test";
			final String datasetSuffix = "group/dataset";
			final List<String> datasetList3 = Arrays.asList(n5.deepList(prefix));
			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", datasetList3.contains("group/" + subGroup));
			Assert.assertTrue("deepList contents", datasetList3.contains(datasetName.replaceFirst(prefix + "/", "")));

			// parallel deepList tests
			final List<String> datasetListP = Arrays.asList(n5.deepList("/", Executors.newFixedThreadPool(2)));
			for (final String subGroup : subGroupNames)
				Assert
						.assertTrue(
								"deepList contents",
								datasetListP.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			Assert.assertTrue("deepList contents", datasetListP.contains(datasetName.replaceFirst("/", "")));
			Assert.assertFalse("deepList stops at datasets", datasetListP.contains(datasetName + "/0"));

			final List<String> datasetListP2 = Arrays.asList(n5.deepList("", Executors.newFixedThreadPool(2)));
			for (final String subGroup : subGroupNames)
				Assert
						.assertTrue(
								"deepList contents",
								datasetListP2.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			Assert.assertTrue("deepList contents", datasetListP2.contains(datasetName.replaceFirst("/", "")));
			Assert.assertFalse("deepList stops at datasets", datasetListP2.contains(datasetName + "/0"));

			final List<String> datasetListP3 = Arrays.asList(n5.deepList(prefix, Executors.newFixedThreadPool(2)));
			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", datasetListP3.contains("group/" + subGroup));
			Assert.assertTrue("deepList contents", datasetListP3.contains(datasetName.replaceFirst(prefix + "/", "")));
			Assert.assertFalse("deepList stops at datasets", datasetListP3.contains(datasetName + "/0"));

			// test filtering
			final Predicate<String> isCalledDataset = d -> {
				return d.endsWith("/dataset");
			};
			final Predicate<String> isBorC = d -> {
				return d.matches(".*/[bc]$");
			};

			final List<String> datasetListFilter1 = Arrays.asList(n5.deepList(prefix, isCalledDataset));
			Assert
					.assertTrue(
							"deepList filter \"dataset\"",
							datasetListFilter1.stream().map(x -> prefix + x).allMatch(isCalledDataset));

			final List<String> datasetListFilter2 = Arrays.asList(n5.deepList(prefix, isBorC));
			Assert
					.assertTrue(
							"deepList filter \"b or c\"",
							datasetListFilter2.stream().map(x -> prefix + x).allMatch(isBorC));

			final List<String> datasetListFilterP1 = Arrays
					.asList(n5.deepList(prefix, isCalledDataset, Executors.newFixedThreadPool(2)));
			Assert
					.assertTrue(
							"deepList filter \"dataset\"",
							datasetListFilterP1.stream().map(x -> prefix + x).allMatch(isCalledDataset));

			final List<String> datasetListFilterP2 = Arrays
					.asList(n5.deepList(prefix, isBorC, Executors.newFixedThreadPool(2)));
			Assert
					.assertTrue(
							"deepList filter \"b or c\"",
							datasetListFilterP2.stream().map(x -> prefix + x).allMatch(isBorC));

			// test dataset filtering
			final List<String> datasetListFilterD = Arrays.asList(n5.deepListDatasets(prefix));
			Assert
					.assertTrue(
							"deepListDataset",
							datasetListFilterD.size() == 1
									&& (prefix + "/" + datasetListFilterD.get(0)).equals(datasetName));
			Assert
					.assertArrayEquals(
							datasetListFilterD.toArray(),
							n5
									.deepList(
											prefix,
											a -> {
												try {
													return n5.datasetExists(a);
												} catch (final IOException e) {
													return false;
												}
											}));

			final List<String> datasetListFilterDandBC = Arrays.asList(n5.deepListDatasets(prefix, isBorC));
			Assert.assertTrue("deepListDatasetFilter", datasetListFilterDandBC.size() == 0);
			Assert
					.assertArrayEquals(
							datasetListFilterDandBC.toArray(),
							n5
									.deepList(
											prefix,
											a -> {
												try {
													return n5.datasetExists(a) && isBorC.test(a);
												} catch (final IOException e) {
													return false;
												}
											}));

			final List<String> datasetListFilterDP = Arrays
					.asList(n5.deepListDatasets(prefix, Executors.newFixedThreadPool(2)));
			Assert
					.assertTrue(
							"deepListDataset Parallel",
							datasetListFilterDP.size() == 1
									&& (prefix + "/" + datasetListFilterDP.get(0)).equals(datasetName));
			Assert
					.assertArrayEquals(
							datasetListFilterDP.toArray(),
							n5
									.deepList(
											prefix,
											a -> {
												try {
													return n5.datasetExists(a);
												} catch (final IOException e) {
													return false;
												}
											},
											Executors.newFixedThreadPool(2)));

			final List<String> datasetListFilterDandBCP = Arrays
					.asList(n5.deepListDatasets(prefix, isBorC, Executors.newFixedThreadPool(2)));
			Assert.assertTrue("deepListDatasetFilter Parallel", datasetListFilterDandBCP.size() == 0);
			Assert
					.assertArrayEquals(
							datasetListFilterDandBCP.toArray(),
							n5
									.deepList(
											prefix,
											a -> {
												try {
													return n5.datasetExists(a) && isBorC.test(a);
												} catch (final IOException e) {
													return false;
												}
											},
											Executors.newFixedThreadPool(2)));

		} catch (final IOException | InterruptedException | ExecutionException e) {
//		} catch (final IOException e) {
			fail(e.getMessage());
		}
	}
}
