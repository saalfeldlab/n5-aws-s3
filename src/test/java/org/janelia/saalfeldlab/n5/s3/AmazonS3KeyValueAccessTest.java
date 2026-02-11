package org.janelia.saalfeldlab.n5.s3;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.kva.AbstractKeyValueAccessTest;
import org.janelia.saalfeldlab.n5.s3.mock.MockS3Factory;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.time.Duration;
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

	@Test
	@Ignore("This test queries actual public s3 buckets, and should only be run manually.")
	public void publicS3URITest() {
		/*more curated example at: https://ome.github.io/ome2024-ngff-challenge/ */
		final String[] testUrls = new String[]{
				"https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.5/idr0033A/BR00109990_C2.zarr",
				"s3://idr/zarr/v0.5/idr0033A/BR00109990_C2.zarr",
				"https://uk1s3.embassy.ebi.ac.uk/idr/share/ome2024-ngff-challenge/idr0012/HT52.ome.zarr",
				"https://uk1s3.embassy.ebi.ac.uk/ebi-ngff-challenge-2024/eea08413-3c5e-4fae-b4fb-02c7c5ebb8a4.zarr",
				"https://radosgw.public.os.wwu.de/n4bi-wp1/challenge/crick/EM04534_04_dU_SBF_8bits_aligned_z4-136_7-7-50nm_v3.zarr",
				"https://demo.data2-brain.esc.rzg.mpg.de/data/zarr3_experimental/MPI_Brain_Research/H6_4S_FCx_VG_synapses_v1_typ_soma_exclusion_v2/color",
		};

		for (String testUrl : testUrls) {
			S3Client s3 = AmazonS3Utils.createS3(testUrl, builder -> {
				builder.region(null);
				builder.overrideConfiguration(ClientOverrideConfiguration.builder()
						.apiCallTimeout(Duration.ofSeconds(30))         // total time including retries
						.apiCallAttemptTimeout(Duration.ofSeconds(25))  // each attempt bound
						.retryStrategy(RetryMode.defaultRetryMode())
						.build());
			});
			URI uri = URI.create(testUrl);
			KeyValueAccess kva = new AmazonS3KeyValueAccess(s3, uri, false);
			System.out.println("KVA valid for " + uri);
		}
	}
}
