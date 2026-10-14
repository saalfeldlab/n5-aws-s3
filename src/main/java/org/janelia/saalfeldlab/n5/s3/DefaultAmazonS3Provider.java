package org.janelia.saalfeldlab.n5.s3;

import org.janelia.saalfeldlab.n5.KeyValueRoot;
import org.janelia.saalfeldlab.n5.N5Exception;

import org.janelia.saalfeldlab.n5.universe.factory.spi.AmazonS3Provider;
import org.janelia.saalfeldlab.n5.universe.factory.spi.AmazonS3Options;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.function.Function;

/**
 * Default {@link AmazonS3Provider}, backed by {@code n5-aws-s3} and the AWS
 * SDK.
 * <p>
 * Stateless: everything it needs comes from the {@link AmazonS3Options} passed
 * in, and the per-location state (the S3 client) lives in the
 * {@link AmazonS3Provider.Endpoint} it hands out. That matters because a
 * provider is a shared, {@code ServiceLoader}-discovered singleton while
 * options are per-factory — a client cached on the provider would have to be
 * keyed by both.
 */
public class DefaultAmazonS3Provider implements AmazonS3Provider {

	@Override
	public AmazonS3Provider.Endpoint endpoint(final URI uri, final AmazonS3Options options) {

		return new DefaultEndpoint(uri, options);
	}

	/**
	 * The client is built lazily and at most once, so {@link #isS3()} followed
	 * by {@link #createKeyValueRoot} costs a single client. Not thread safe,
	 * and not meant to be: one handle belongs to one
	 * {@code createKeyValueRoot} dispatch.
	 */
	private static class DefaultEndpoint implements AmazonS3Provider.Endpoint {

		private final URI uri;

		private final AmazonS3Options options;

		private S3Client client = null;

		DefaultEndpoint(final URI uri, final AmazonS3Options options) {

			this.uri = uri;
			this.options = options;
		}

		@Override
		public boolean isS3() {

			/*
			 * Ask for the ACL of a bucket that almost certainly does not exist. A
			 * reply of any kind means we are talking to something that speaks S3.
			 */
			try {
				client().getBucketAcl(builder -> builder.bucket("" + System.nanoTime()));
				return true;
			} catch (final Throwable e) {
				closeClient();
				return false;
			}
		}

		@Override
		public KeyValueRoot createKeyValueRoot(final boolean readOnly) {

			final String bucket = AmazonS3Utils.getS3Bucket(uri);
			final String root = AmazonS3Utils.getS3Key(uri);
			try {
				final KeyValueRoot kvr = new AmazonS3KeyValueRoot(client(), bucket, root, !readOnly);
				client = null; // kvr owns client now
				return kvr;
			} catch (final Exception e) {
				closeClient();
				throw e;
			}
		}

		private void closeClient() {

			// NB: options.clientProvider is used to inject a S3Client
			// for tests. Never close an injected client.
			if (client != null && options.clientProvider() == null) {
				client.close();
				client = null;
			}
		}

		private S3Client client() {

			if (client == null)
				client = createClient();
			return client;
		}

		/*
		 * To take over client creation, set AmazonS3Options.clientProvider.
		 */
		private S3Client createClient() {

			final Function<URI, S3Client> clientProvider = options.clientProvider();
			if (clientProvider != null)
				return clientProvider.apply(uri);

			try {
				return AmazonS3Utils.createS3(uri.toString(), options.builderConfig());
			} catch (final Throwable e) {
				throw new N5Exception("Could not create s3 client from uri: " + uri, e);
			}
		}
	}
}
