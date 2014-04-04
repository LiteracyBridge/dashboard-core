package org.literacybridge.dashboard.services;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

/**
 */
@Service("s3Service")
public class S3Service {

  @Value("${amazon.s3.userEmail}")
  private String userEmail;

  @Value("${amazon.s3.accessKeyID}")
  private String accessKeyId;

  @Value("${amazon.s3.secretAccessKey}")
  private String secretAccessKey;

  @Value("${amazon.s3.uploadBucket}")
  private String uploadBucket;

  @Value("${amazon.s3.aggregationBucket}")
  private String aggregationBucket;

  protected AWSCredentials getCredentials() {
    return new BasicAWSCredentials(accessKeyId, secretAccessKey);
  }

  public String getUploadBucket() {
    return uploadBucket;
  }

  public String getAggregationBucket() {
    return aggregationBucket;
  }

  public AmazonS3Client getClient() {
    return new AmazonS3Client(getCredentials());
  }

  public boolean doesObjectExist(String bucket, String key)  {

    final AmazonS3Client client = getClient();

    boolean retVal;
    try {
      final ObjectMetadata metadata = client.getObjectMetadata(bucket, key);
      retVal = (metadata != null);
    } catch (AmazonS3Exception s3e) {
      if (s3e.getStatusCode() == 404) {
        retVal = false;
      } else {
        throw s3e;
      }
    }

    return retVal;
  }

  public boolean writeZipObject(String bucket, String id, InputStream is, long length, Map<String, String> userMetadata) {
    final AmazonS3Client    client = getClient();

    //
    //  Now write the initial file to S3
    final ObjectMetadata newMetadata = new ObjectMetadata();
    newMetadata.setContentLength(length);
    for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
      newMetadata.addUserMetadata(entry.getKey(), entry.getValue());
    }
    newMetadata.setContentType("application/zip");

    final PutObjectRequest request = new PutObjectRequest(bucket, id, is, newMetadata);
    /*
    AccessControlList acl = new AccessControlList();
    acl.grantPermission(new EmailAddressGrantee(userEmail), Permission.FullControl);

    request.withAccessControlList(acl);
        */
    final PutObjectResult result = client.putObject(request);
    return true;
  }

  public ObjectMetadata getObject(String bucket, String key, File file) {
    final AmazonS3Client client = getClient();

    GetObjectRequest  getObjectRequest = new GetObjectRequest(bucket, key);
    return client.getObject(getObjectRequest, file);
  }



}
