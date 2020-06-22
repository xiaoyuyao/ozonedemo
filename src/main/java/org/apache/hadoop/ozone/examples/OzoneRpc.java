/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

/**
 * A mini demo program that access Ozone Volume/Bucket/Key via Ozone RPC
 * with minimal dependency on classpath/configuration stuff.
 * It has been tested with secure, non-secure, HA and non-HA Ozone clusters.
 */
public class OzoneRpc {

  static OzoneClient getOzoneClient(boolean secure, String omServiceId) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    if (omServiceId != null) {
      conf.set("ozone.om.service.ids", omServiceId);
      // TODO: If you have OM HA configured, change the following as appropriate.
      conf.set("ozone.om.address.ozone1.om1", "xyao-ozs-1.xyao-ozs.root.hwx.site:9862");
      conf.set("ozone.om.address.ozone1.om2", "xyao-ozs-2.xyao-ozs.root.hwx.site:9862");
      conf.set("ozone.om.address.ozone1.om3", "xyao-ozs-3.xyao-ozs.root.hwx.site:9862");
    } else {
      // TODO: If you don't have OM HA configured, change the following as appropriate.
      conf.set("ozone.om.address", "xyao-ozs-1.xyao-ozs.root.hwx.site:9862");
    }
    if (secure) {
      conf.set("hadoop.security.authentication", "kerberos");
      conf.set("ozone.om.kerberos.principal", "om/_HOST@ROOT.HWX.SITE");
      UserGroupInformation.setConfiguration(conf);
      // TODO: If you have Hadoop/Ozone security enabled, customize the principal and keytab as appropriate.
      String principal = "om/xyao-ozs-1.xyao-ozs.root.hwx.site@ROOT.HWX.SITE";
      String keytab = "/tmp/ozone.keytab";
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
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
      // Get an Ozone RPC Client.
      ozoneClient = getOzoneClient(true, omServiceId);

      // An Ozone ObjectStore instance is the entry point to access Ozone.
      ObjectStore store = ozoneClient.getObjectStore();

      // Create volume with random name.
      String testVolumeName = "testvolume-" + getRandomString(4);
      store.createVolume(testVolumeName);
      OzoneVolume volume = store.getVolume(testVolumeName);
      System.out.println("Volume " + testVolumeName + " created.");

      // Create bucket with random name.
      String testBucketName = "testbucket-" + getRandomString(4);
      volume.createBucket(testBucketName);
      OzoneBucket bucket = volume.getBucket(testBucketName);
      System.out.println("Bucket " + testBucketName + " created.");

      // Write key with random name.
      String testKeyName = "testkey-" + getRandomString(4);
      String fileContentWrite = "This is a test key1.";
      try (OzoneOutputStream out = bucket.createKey(testKeyName, 1024,
          ReplicationType.STAND_ALONE, ReplicationFactor.ONE, new HashMap<>())) {
        out.write(fileContentWrite.getBytes());
      }
      System.out.println("Key " + testKeyName + " created.");

      // Read key
      byte[] fileContentRead;
      int lenRead = 0;
      try (OzoneInputStream in = bucket.readKey(testKeyName)) {
        fileContentRead = new byte[fileContentWrite.getBytes().length];
        lenRead = in.read(fileContentRead);
      }
      System.out.println("Key " + testKeyName + " read return " + lenRead +
          " bytes");

      // Compare the content from read with those written.
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
      // Ensure Ozone client resource is safely closed at the end.
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
