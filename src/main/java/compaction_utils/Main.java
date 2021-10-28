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

import java.util.Arrays;

public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Need parameters!\n\tg: to generate data\n\tc: to cache verify data\n\tv: to verify data");
      System.exit(-1);
    }
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    try {
      switch (args[0]) {
        case "g":
          generateData(session);
          break;
        case "c":
          cachedVerifyData(session);
          break;
        case "v":
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

  public static void generateData(Session session) throws  Exception{
    deleteSg(session);
    session.setStorageGroup(compaction_utils.Constant.SG_NAME);
    compaction_utils.DataGenerationUtils.generateUnseqData(session, 11, 0, 5, 5, 2000);
  }

  public static void cachedVerifyData(Session session) throws Exception {
    compaction_utils.DataVerifyUtils.cacheQueryResult(session);
  }
}
