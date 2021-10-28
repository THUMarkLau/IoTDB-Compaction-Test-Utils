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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerationUtils {

  public static void generateUnseqData(Session session, int seqFileNum, int unseqFileNum, int deviceNum, int measurementNum, long pointInFileForEachTS) throws IoTDBConnectionException, StatementExecutionException {
    long startTimeForSeqData = pointInFileForEachTS * unseqFileNum;
    long endTimeForSeqData = startTimeForSeqData + pointInFileForEachTS * seqFileNum;
    for(long time = startTimeForSeqData; time < endTimeForSeqData;) {
      for(int i = 0; i < deviceNum; ++i) {
        List<MeasurementSchema> schemaList = new ArrayList<>();
        for(int j = 0; j < measurementNum; ++j) {
          schemaList.add(new MeasurementSchema(String.format(Constant.MEASUREMENT_PATTERN, j), TSDataType.INT32));
        }
        Tablet tablet = new Tablet(Constant.getDeviceName(i), schemaList, 100);
        for (long row = time; row < time + 100; row++) {
          int rowIndex = tablet.rowSize++;
          tablet.addTimestamp(rowIndex, row);
          for (int j = 0; j < measurementNum; ++j) {
            int value = new Random().nextInt() % 100000;
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
      time += 100;
      if (time != startTimeForSeqData && time % pointInFileForEachTS == 0) {
        session.executeNonQueryStatement("flush");
      }
    }
    session.executeNonQueryStatement("flush");
    for(long time = 0; time < startTimeForSeqData;) {
      for(int i = 0; i < deviceNum; ++i) {
        List<MeasurementSchema> schemaList = new ArrayList<>();
        for(int j = 0; j < measurementNum; ++j) {
          schemaList.add(new MeasurementSchema(String.format(Constant.MEASUREMENT_PATTERN, j), TSDataType.INT32));
        }
        Tablet tablet = new Tablet(Constant.getDeviceName(i), schemaList, 100);
        for (long row = time; row < time + 100; row++) {
          int rowIndex = tablet.rowSize++;
          tablet.addTimestamp(rowIndex, row);
          for (int j = 0; j < measurementNum; ++j) {
            int value = new Random().nextInt();
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
      time += 100;
      if (time % pointInFileForEachTS == 0) {
        session.executeNonQueryStatement("flush");
      }
    }
    session.executeNonQueryStatement("flush");
  }
}
