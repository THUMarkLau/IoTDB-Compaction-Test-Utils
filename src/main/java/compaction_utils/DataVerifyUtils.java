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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DataVerifyUtils {
  private static String[] cachedData = null;

  public static void cacheQueryResult(Session session)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    SessionDataSet dataSet =
        session.executeQueryStatement(
            String.format(
                "select count(*), avg(*), max_value(*), min_value(*), sum(*) from %s.**",
                Constant.SG_NAME));
    File cachedFile = new File(Constant.VERIFY_CACHED_FILE);
    if (cachedFile.exists()) {
      cachedFile.delete();
    }
    BufferedOutputStream os =
        new BufferedOutputStream(new FileOutputStream(Constant.VERIFY_CACHED_FILE));
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      for (Field field : fields) {
        os.write(
            (field.getObjectValue(field.getDataType()) + " ").getBytes(StandardCharsets.UTF_8));
      }
    }
    os.flush();
  }

  private static void loadCachedQueryResult() throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(Constant.VERIFY_CACHED_FILE))) {
      String lineData = reader.readLine();
      cachedData = lineData.split(" ");
    }
  }

  public static void verify(Session session) throws Exception {
    loadCachedQueryResult();
    SessionDataSet dataSet =
        session.executeQueryStatement(
            String.format(
                "select count(*), avg(*), max_value(*), min_value(*), sum(*) from %s.**",
                Constant.SG_NAME));
    int currentIdx = 0;
    boolean wrongData = false;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      for (Field field : fields) {
        if (field.getStringValue().equals("null")) {
          if (!field.getStringValue().equals(cachedData[currentIdx])) {
            System.out.printf(
                "Error!! Cached data is %s, but %s given\n",
                cachedData[currentIdx], field.getObjectValue(field.getDataType()));
            wrongData = true;
          }
          currentIdx++;
          continue;
        }
        if (Double.parseDouble(field.getObjectValue(field.getDataType()).toString())
                - Double.parseDouble(cachedData[currentIdx])
            > Constant.VERIFY_ERROR) {
          System.out.printf(
              "Error!! Cached data is %s, but %s given\n",
              cachedData[currentIdx], field.getObjectValue(field.getDataType()));
          wrongData = true;
        }
        currentIdx++;
      }
      if (!wrongData) {
        System.out.println("All data is verified!");
      }
    }
  }
}
