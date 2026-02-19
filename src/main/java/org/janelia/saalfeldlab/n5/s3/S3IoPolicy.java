package org.janelia.saalfeldlab.n5.s3;

import org.janelia.saalfeldlab.n5.IoPolicy;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.List;
import java.util.stream.Collectors;

import static org.janelia.saalfeldlab.n5.s3.AmazonS3KeyValueAccess.*;

public interface S3IoPolicy extends IoPolicy {

    class Unsafe implements S3IoPolicy {

        protected final S3Client s3;
        protected final String bucketName;

        public Unsafe(S3Client s3, String bucketName) {
            this.s3 = s3;
            this.bucketName = bucketName;
        }

        @Override
        public void write(String key, ReadData readData) {

            final PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            try {
                s3.putObject(putRequest, RequestBody.fromByteBuffer(readData.toByteBuffer()));
            } catch (S3Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public VolatileReadData read(String key) {
            return VolatileReadData.from(new S3LazyRead(s3, bucketName, key, false));
        }

        @Override
        public void delete(String key) {
            if (!key.endsWith("/")) {

                final DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();

                try {
                    s3.deleteObject(deleteRequest);
                } catch (S3Exception e) {
                }
            }

            final String prefix = addTrailingSlash(key);

            ListObjectsV2Request listObjectsRequest;
            ListObjectsV2Response objectsListing;
            listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .build();

            do {
                objectsListing = s3.listObjectsV2(listObjectsRequest);
                final List<ObjectIdentifier> objectsToDelete = objectsListing.contents().stream().map(x -> {
                    return ObjectIdentifier.builder().key(x.key()).build();
                }).collect(Collectors.toList());

                if (!objectsToDelete.isEmpty()) {
                    final DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                            .bucket(bucketName)
                            .delete(Delete.builder().objects(objectsToDelete).build())
                            .build();

                    s3.deleteObjects(deleteRequest);
                }

                // TODO what about continuation token?

            } while (objectsListing.isTruncated());
        }
    }

    class EtagMatch extends Unsafe {

        public EtagMatch(S3Client s3, String bucketName) {
            super(s3, bucketName);
        }

        @Override
        public VolatileReadData read(String key) {
            return VolatileReadData.from(new S3LazyRead(s3, bucketName, key, true));
        }
    }

    class S3LazyRead implements LazyRead {

        private final String s3Key;
        private final boolean verifyEtag;
        private final S3Client s3;
        private final String bucketName;
        private String eTag = null;


        S3LazyRead(final S3Client s3, final String bucketName, final String s3Key, final boolean verifyEtag) {
            this.s3 = s3;
            this.bucketName = bucketName;
            this.s3Key = s3Key;
            this.verifyEtag = verifyEtag;
        }

        private GetObjectRequest createObjectRequest(final String s3Key, long offset, long length) {


            final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                    .key(s3Key)
                    .bucket(bucketName);

            // Only add range header if we're doing a partial read
            if (offset > 0 || length > 0) {
                // HTTP Range header format: "bytes=start-end"
                // If length is 0 or negative, read from offset to end of file
                final String range = length > 0
                        ? String.format("bytes=%d-%d", offset, offset + length - 1)
                        : String.format("bytes=%d-", offset);
                requestBuilder.range(range);
            }

            if (verifyEtag && eTag != null)
                requestBuilder.ifMatch(eTag);

            return requestBuilder.build();
        }

        @Override public ReadData materialize(long offset, long length) throws N5Exception.N5IOException {

            final ResponseBytes<GetObjectResponse> response = rethrowS3Exceptions(() -> {
                final GetObjectRequest request = createObjectRequest(s3Key, offset, length);
                ResponseBytes<GetObjectResponse> responseBytes = s3.getObject(request, ResponseTransformer.toBytes());
                if (verifyEtag && eTag == null)
                    eTag = responseBytes.response().eTag();
                return responseBytes;
            });
            return ReadData.from(response.asByteArray());
        }

        @Override public long size() throws N5Exception.N5IOException {

            final HeadObjectResponse response = headObjectRequest(s3, bucketName, s3Key, eTag);

            if (verifyEtag && eTag == null)
                eTag = response.eTag();

            return response.contentLength();
        }

        @Override
        public void close() {
            eTag = null;
        }
    }
}
