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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class DataGenerationUtils {
  static TestProperties testProperties = TestProperties.getInstance();

  public static void generateNotOverlapNotAlignedData(
      Session session,
      int seqFileNum,
      int unseqFileNum,
      int deviceNum,
      int measurementNum,
      long pointInFileForEachTS)
      throws IoTDBConnectionException, StatementExecutionException {
    long startTimeForSeqData = pointInFileForEachTS * unseqFileNum;
    long endTimeForSeqData = startTimeForSeqData + pointInFileForEachTS * seqFileNum;
    for (long time = startTimeForSeqData; time < endTimeForSeqData; time += pointInFileForEachTS) {
      writeNotAlignedTsFile(deviceNum, measurementNum, time, time + pointInFileForEachTS, session);
    }
    session.executeNonQueryStatement("flush");
    for (long time = 0; time < startTimeForSeqData; time += pointInFileForEachTS) {
      writeNotAlignedTsFile(deviceNum, measurementNum, time, time + pointInFileForEachTS, session);
    }
    session.executeNonQueryStatement("flush");
  }

  public static void generateOverlapNotAlignedData(
      Session session,
      int seqFileNum,
      int unseqFileNum,
      int deviceNum,
      int measurementNum,
      long pointInFileForEachTS)
      throws IoTDBConnectionException, StatementExecutionException {
    long endTimeForSeqData = pointInFileForEachTS * seqFileNum;
    for (long time = 0; time < endTimeForSeqData; time += pointInFileForEachTS) {
      writeNotAlignedTsFile(deviceNum, measurementNum, time, time + pointInFileForEachTS, session);
      session.executeNonQueryStatement("flush");
    }
    session.executeNonQueryStatement("flush");
    for (long time = 0; time < pointInFileForEachTS * unseqFileNum; time += pointInFileForEachTS) {
      writeNotAlignedTsFile(deviceNum, measurementNum, time, time + pointInFileForEachTS, session);
      session.executeNonQueryStatement("flush");
    }
    session.executeNonQueryStatement("flush");
  }

  public static void writeNotAlignedTsFile(
      int deviceNum, int measurementNum, long startTime, long endTime, Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    boolean useRandom = Boolean.parseBoolean(testProperties.getProperty("use-random"));
    Random random = new Random();
    boolean useRandomSchema =
        Boolean.parseBoolean(testProperties.getProperty("diff-schema-in-each-tsfile"));
    System.out.println(useRandomSchema);
    for (int i = 0; i < deviceNum; ++i) {
      List<MeasurementSchema> schemaList = new ArrayList<>();
      Set<Integer> schemaIdxSet = new HashSet<>();
      for (int j = 0; j < measurementNum; ++j) {
        int chosenIdx = useRandomSchema ? random.nextInt(measurementNum) : j;
        if (schemaIdxSet.contains(chosenIdx)) {
          continue;
        }
        schemaList.add(
            new MeasurementSchema(
                String.format(Constant.MEASUREMENT_PATTERN, chosenIdx), TSDataType.INT32));
        schemaIdxSet.add(chosenIdx);
      }
      Tablet tablet = new Tablet(Constant.getDeviceName(i), schemaList, 100);
      for (long row = startTime; row < endTime; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, row);
        for (int j = 0; j < schemaList.size(); ++j) {
          int value = useRandom ? random.nextInt() % 100000 : (int) row;
          tablet.addValue(schemaList.get(j).getMeasurementId(), rowIndex, value);
        }
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertTablet(tablet, true);
          tablet.reset();
        }
      }
      if (tablet.rowSize != 0) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }
    session.executeNonQueryStatement("FLUSH");
  }

  public static void generateNotOverlapAlignedData(
      Session session,
      int seqFileNum,
      int unseqFileNum,
      int deviceNum,
      int measurementNum,
      long pointInFileForEachTS)
      throws IoTDBConnectionException, StatementExecutionException {
    long startTimeForSeqData = pointInFileForEachTS * unseqFileNum;
    long endTimeForSeqData = startTimeForSeqData + pointInFileForEachTS * seqFileNum;
    AlignedGenerationResource resource = new AlignedGenerationResource();
    registerAlignedSeries(deviceNum, measurementNum, session, resource);
    for (long time = startTimeForSeqData; time < endTimeForSeqData; time += pointInFileForEachTS) {
      writeAlignedTsFile(time, time + pointInFileForEachTS, resource, session);
    }
    session.executeNonQueryStatement("flush");
    for (long time = 0; time < startTimeForSeqData; time += pointInFileForEachTS) {
      writeAlignedTsFile(time, time + pointInFileForEachTS, resource, session);
      session.executeNonQueryStatement("flush");
    }
    session.executeNonQueryStatement("flush");
  }

  public static void generateOverlapAlignedData(
      Session session,
      int seqFileNum,
      int unseqFileNum,
      int deviceNum,
      int measurementNum,
      long pointInFileForEachTS)
      throws IoTDBConnectionException, StatementExecutionException {
    long endTimeForSeqData = pointInFileForEachTS * seqFileNum;
    AlignedGenerationResource resource = new AlignedGenerationResource();
    registerAlignedSeries(deviceNum, measurementNum, session, resource);
    for (long time = 0; time < endTimeForSeqData; time += pointInFileForEachTS) {
      writeAlignedTsFile(time, time + pointInFileForEachTS, resource, session);
      session.executeNonQueryStatement("flush");
    }
    session.executeNonQueryStatement("flush");
    for (long time = 0; time < pointInFileForEachTS * unseqFileNum; time += pointInFileForEachTS) {
      writeAlignedTsFile(time, time + pointInFileForEachTS, resource, session);
      session.executeNonQueryStatement("flush");
    }
    session.executeNonQueryStatement("flush");
  }

  public static void registerAlignedSeries(
      int deviceNum, int seriesNum, Session session, AlignedGenerationResource resource)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < deviceNum; ++i) {
      if (!resource.init) {
        for (int j = 0; j < seriesNum; ++j) {
          resource.measurements.add("s" + j);
          resource.dataTypes.add(TSDataType.INT32);
          resource.encodings.add(TSEncoding.PLAIN);
          resource.compressionTypes.add(CompressionType.SNAPPY);
          resource.aliasList.add("s" + j);
          resource.schemaList.add(
              new MeasurementSchema(
                  resource.measurements.get(j),
                  resource.dataTypes.get(j),
                  resource.encodings.get(j),
                  resource.compressionTypes.get(j)));
        }
        resource.init = true;
      }
      String deviceId = Constant.getDeviceName(i);
      resource.deviceIds.add(deviceId);
      session.createAlignedTimeseries(
          deviceId,
          resource.measurements,
          resource.dataTypes,
          resource.encodings,
          resource.compressionTypes,
          resource.aliasList);
    }
  }

  public static void writeAlignedTsFile(
      long startTime, long endTime, AlignedGenerationResource resource, Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    boolean useRandom = Boolean.parseBoolean(testProperties.getProperty("use-random"));
    Random random = new Random();
    boolean useRandomSchema =
        Boolean.parseBoolean(testProperties.getProperty("diff-schema-in-each-tsfile"));
    for (String device : resource.deviceIds) {
      List<MeasurementSchema> schemaList = new ArrayList<>();
      if (useRandomSchema) {
        Set<Integer> randomSchemaIdxSet = new HashSet<>();
        for (int i = 0; i < resource.schemaList.size(); ++i) {
          int idx = random.nextInt(resource.schemaList.size());
          if (randomSchemaIdxSet.contains(idx)) {
            continue;
          }
          schemaList.add(resource.schemaList.get(idx));
          randomSchemaIdxSet.add(idx);
        }
      } else {
        schemaList = resource.schemaList;
      }
      Tablet tablet = new Tablet(device, schemaList);
      tablet.setAligned(true);
      for (long row = startTime; row < endTime; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, row);
        for (MeasurementSchema schema : schemaList) {
          int value = useRandom ? random.nextInt() % 100000 : (int) row;
          tablet.addValue(schema.getMeasurementId(), rowIndex, value);
        }
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertTablet(tablet, true);
          tablet.reset();
        }
      }
      if (tablet.rowSize > 0) {
        session.insertTablet(tablet, true);
      }
    }
  }

  public static void generateNotOverlapMixData(
      Session session,
      int seqNum,
      int unseqNum,
      int deviceNum,
      int measurementNum,
      long pointInFileForEachTS)
      throws IoTDBConnectionException, StatementExecutionException {}

  public static void generateOverlapMixData(
      Session session,
      int seqNum,
      int unseqNum,
      int deviceNum,
      int measurementNum,
      long pointInFileForEachTS)
      throws IoTDBConnectionException, StatementExecutionException {}
}
