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

package org.apache.hadoop.hbase.regionserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class IOCalculate {
  private static final Logger LOG = LoggerFactory.getLogger(IOCalculate.class);
  private static long AppIO = 0;
  private static long DiskIO = 0;
  private static long realDiskIO = 0;
  private static int AppPage = 0;
  private static int DiskPage = 0;
  private static int case1 = 0;
  private static int case2 = 0;
  private static int case3 = 0;
  private static long IO_Time = 0;
  private static int IO_Access = 0;
  private static int cache_Access = 0;
  private static long Duration_Time = 0;
  private static long IOperRequest = 0;
  private static final long NANO_TIME = TimeUnit.MILLISECONDS.toNanos(1);

  public IOCalculate() {
  }
  
  public void add_AppIO(int IO) {
    AppIO += IO;
  }

  public void add_AppPage(int pageNum) {
    AppPage += pageNum;
  }

  public void add_DiskIO(int IO) {
    DiskIO += IO;
  }

  public void add_DiskPage(int pageNum) {
    DiskPage += pageNum;
  }

  public void add_realDiskIO(long IO) {
    realDiskIO += IO;
  }

  public void add_case1(int access) {
    case1 += access;
  }

  public void add_case2(int access) {
    case2 += access;
  }

  public void add_case3(int access) {
    case3 += access;
  }

  public void add_ioTime(long Time) {
    IO_Time += Time;
  }

  public void add_Duration(long Time) {
    Duration_Time += Time;
  }

  public void add_ioAccess(int access) {
    IO_Access += access;
  }

  public void add_cacheHit(int access) {
    cache_Access += access;
  }

  public long get_AppIO() {
    return AppIO;
  }

  public long get_DiskIO() {
    return DiskIO;
  }

  public int get_AppPage() {
    return AppPage;
  }

  public int get_DiskPage() {
    return DiskPage;
  }

  public long get_realDiskIO() {
    return realDiskIO;
  }

  public long get_case1() {
    return case1;
  }

  public long get_case2() {
    return case2;
  }

  public long get_case3() {
    return case3;
  }

  public long get_ioTime() {
    return IO_Time/NANO_TIME;
  }

  public int get_ioAccess() {
    return IO_Access;
  }

  public int get_cacheAccess() {
    return cache_Access;
  }

  public long get_Duration(){
    return Duration_Time/NANO_TIME;
  }
}

