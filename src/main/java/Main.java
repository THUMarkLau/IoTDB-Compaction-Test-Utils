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

import org.apache.iotdb.session.Session;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.util.Scanner;

public class Main {
  public static void main(String[] args) throws Exception {
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);
//    deleteSg(session);
    try {
//      session.setStorageGroup(Constant.SG_NAME);
//      DataGenerationUtils.generateUnseqData(session, 3, 5, 3, 5, 1900);
//      DataVerifyUtils.cacheQueryResult(session);
      DataVerifyUtils.verify(session);
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
}
