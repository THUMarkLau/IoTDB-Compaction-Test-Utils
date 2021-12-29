package compaction_utils;
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

public class Constant {
  public static final String SG_NAME = "root.test.g_0";
  public static final String DEVICE_PATTERN = "d%d";
  public static final String MEASUREMENT_PATTERN = "s%d";
  public static final String INSERT_SQL_PATTERN = "insert into %s(timestamp, %s), values (%d, %d)";
  public static final String FLUSH_SQL = "flush";
  public static final String VERIFY_CACHED_FILE = "verify_cache.txt";
  public static final double VERIFY_ERROR = 0.0001;

  public static String getDeviceName(int deviceId) {
    return String.format(SG_NAME.concat(".") + DEVICE_PATTERN, deviceId);
  }

  public static String getMeasurementName(int deviceId, int measurementId) {
    return String.format(
        SG_NAME.concat(".") + DEVICE_PATTERN.concat(".") + MEASUREMENT_PATTERN,
        deviceId,
        measurementId);
  }

  public static String getInsertSql(int deviceId, int measurementId, int timestamp, int value) {
    return String.format(
        INSERT_SQL_PATTERN,
        getDeviceName(deviceId),
        String.format(MEASUREMENT_PATTERN, measurementId),
        timestamp,
        value);
  }
}
