package org.apache.hadoop.ozone.examples;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.HashMap;

public class OzoneRpc {

  static OzoneClient getOzoneClient(boolean secure, String omServiceId) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    if (omServiceId != null) {
      conf.set("ozone.om.service.ids", omServiceId);
      conf.set("ozone.om.address.ozone1.om1", "xyao-ozs-1.xyao-ozs.root.hwx.site:9862");
      conf.set("ozone.om.address.ozone1.om2", "xyao-ozs-2.xyao-ozs.root.hwx.site:9862");
      conf.set("ozone.om.address.ozone1.om3", "xyao-ozs-3.xyao-ozs.root.hwx.site:9862");
    } else {
      conf.set("ozone.om.address", "xyao-ozs-1.xyao-ozs.root.hwx.site:9862");
    }
    if (secure) {
      conf.set("hadoop.security.authentication", "kerberos");
      conf.set("ozone.om.kerberos.principal", "om/_HOST@ROOT.HWX.SITE");
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab("om/xyao-ozs-1.xyao-ozs.root.hwx.site@ROOT.HWX.SITE","/tmp/ozone.keytab");
    }
    return omServiceId != null ?
        OzoneClientFactory.getRpcClient(omServiceId, conf) :
        OzoneClientFactory.getClient(conf);
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Ozone Rpc Demo Begin.");
    OzoneClient ozoneClient = null;
    String omServiceId = "ozone1";

    try {
      ozoneClient = getOzoneClient(true, omServiceId);
      ObjectStore store = ozoneClient.getObjectStore();

      // create volume
      String testVolumeName = "testvolume-" + getRandomString(4);
      store.createVolume(testVolumeName);
      OzoneVolume volume = store.getVolume(testVolumeName);
      System.out.println("Volume " + testVolumeName + " created.");

      // create bucket
      String testBucketName = "testbucket-" + getRandomString(4);
      volume.createBucket(testBucketName);
      OzoneBucket bucket = volume.getBucket(testBucketName);
      System.out.println("Bucket " + testBucketName + " created.");

      // write key
      String testKeyName = "testkey-" + getRandomString(4);
      String fileContentWrite = "This is a test key1.";
      try (OzoneOutputStream out = bucket.createKey(testKeyName, 1024,
          ReplicationType.STAND_ALONE, ReplicationFactor.ONE, new HashMap<>())) {
        out.write(fileContentWrite.getBytes());
      }
      System.out.println("Key " + testKeyName + " created.");

      // read key
      byte[] fileContentRead;
      int lenRead = 0;
      try (OzoneInputStream in = bucket.readKey(testKeyName)) {
        fileContentRead = new byte[fileContentWrite.getBytes().length];
        lenRead = in.read(fileContentRead);
      }
      System.out.println("Key " + testKeyName + " read return " + lenRead +
          " bytes");

      // compare the content read vs written
      String contentRead = new String(fileContentRead);
      if (fileContentWrite.equals(contentRead)) {
        System.out.println("Read verification done successfully!");
      } else {
        System.err.println("Read verification failed! Original written: " +
            fileContentWrite + " read content: " + fileContentRead);
      }
    } catch (Exception e) {
      System.err.println(e);
    } finally {
      if (ozoneClient != null) {
        ozoneClient.close();
      }
    }
    System.out.println("Ozone Rpc Demo End.");
  }

  private static String getRandomString(int len) {
    return RandomStringUtils.randomAlphanumeric(len).toLowerCase();
  }
}