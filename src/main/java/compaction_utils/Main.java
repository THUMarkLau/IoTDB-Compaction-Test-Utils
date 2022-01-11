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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package compaction_utils;

import org.apache.iotdb.session.Session;

import java.util.Locale;

public class Main {
  public static void main(String[] args) throws Exception {
    TestProperties properties = TestProperties.getInstance();
    String host = properties.getProperty("host");
    int port = Integer.parseInt(properties.getProperty("port"));
    Session session = new Session(host, port, "root", "root");
    session.open(false);

    String option = properties.getProperty("option");
    try {
      switch (option.toLowerCase(Locale.ROOT)) {
        case "generate":
          generateData(session);
          break;
        case "cache-result":
          cachedVerifyData(session);
          break;
        case "verify":
          DataVerifyUtils.verify(session);
          break;
      }
    } finally {
      session.close();
    }
  }

  public static void deleteSg(Session session) {
    try {
      session.executeNonQueryStatement("delete storage group " + Constant.SG_NAME);
    } catch (Exception e) {
    }
  }

  public static void generateData(Session session) throws Exception {
    deleteSg(session);
    session.setStorageGroup(compaction_utils.Constant.SG_NAME);
    TestProperties properties = TestProperties.getInstance();
    int seqFileNum = Integer.parseInt(properties.getProperty("seq_file_num"));
    int unseqFileNum = Integer.parseInt(properties.getProperty("unseq_file_num"));
    int deviceNum = Integer.parseInt(properties.getProperty("device_num"));
    int measurementNum = Integer.parseInt(properties.getProperty("measurement_num"));
    int pointInEachFileForEachMeasurement =
        Integer.parseInt(properties.getProperty("point_num_for_each_measurement_in_each_file"));
    switch (properties.getProperty("generate-mod")) {
      case "NOT-ALIGNED":
        if (!Boolean.parseBoolean(properties.getProperty("overlap"))) {
          compaction_utils.DataGenerationUtils.generateNotOverlapNotAlignedData(
              session,
              seqFileNum,
              unseqFileNum,
              deviceNum,
              measurementNum,
              pointInEachFileForEachMeasurement);
        } else {
          compaction_utils.DataGenerationUtils.generateOverlapNotAlignedData(
              session,
              seqFileNum,
              unseqFileNum,
              deviceNum,
              measurementNum,
              pointInEachFileForEachMeasurement);
        }
        break;
      case "ALIGNED":
        if (!Boolean.parseBoolean(properties.getProperty("overlap"))) {
          compaction_utils.DataGenerationUtils.generateNotOverlapAlignedData(
              session,
              seqFileNum,
              unseqFileNum,
              deviceNum,
              measurementNum,
              pointInEachFileForEachMeasurement);
        } else {
          compaction_utils.DataGenerationUtils.generateOverlapAlignedData(
              session,
              seqFileNum,
              unseqFileNum,
              deviceNum,
              measurementNum,
              pointInEachFileForEachMeasurement);
        }
        break;
      case "MIX":
        if (!Boolean.parseBoolean(properties.getProperty("overlap"))) {
          compaction_utils.DataGenerationUtils.generateNotOverlapMixData(
              session,
              seqFileNum,
              unseqFileNum,
              deviceNum,
              measurementNum,
              pointInEachFileForEachMeasurement);
        } else {
          compaction_utils.DataGenerationUtils.generateOverlapMixData(
              session,
              seqFileNum,
              unseqFileNum,
              deviceNum,
              measurementNum,
              pointInEachFileForEachMeasurement);
        }
        break;
    }
  }

  public static void cachedVerifyData(Session session) throws Exception {
    compaction_utils.DataVerifyUtils.cacheQueryResult(session);
  }
}
