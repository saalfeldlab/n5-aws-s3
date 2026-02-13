package org.janelia.saalfeldlab.n5.s3;

import org.janelia.saalfeldlab.n5.IoPolicy;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.janelia.saalfeldlab.n5.s3.AmazonS3KeyValueAccess.addTrailingSlash;

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
            return VolatileReadData.from(new AmazonS3KeyValueAccess.S3LazyRead(s3, bucketName, key, false));
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
            return VolatileReadData.from(new AmazonS3KeyValueAccess.S3LazyRead(s3, bucketName, key, true));
        }
    }

    ;
}
