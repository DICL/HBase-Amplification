/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Class that implements cache metrics for bucket cache.
 */
@InterfaceAudience.Private
public class BucketCacheStats extends CacheStats {
  private final LongAdder ioHitCount = new LongAdder();
  private final LongAdder ioHitTime = new LongAdder();
  private final LongAdder rowCacheTime = new LongAdder();
  private final LongAdder checkTime = new LongAdder();
  private final LongAdder cacheTime = new LongAdder();
  private final LongAdder skipTime = new LongAdder();
  private final LongAdder cellTime = new LongAdder();
  private final LongAdder bucketCacheTime = new LongAdder();
  private final LongAdder cacheBlockTime = new LongAdder();
  private static final long NANO_TIME = TimeUnit.MILLISECONDS.toNanos(1);
  private long lastLogTime = EnvironmentEdgeManager.currentTime();

  BucketCacheStats() {
    super("BucketCache");
  }

  @Override
  public String toString() {
    return super.toString() + ", ioHitsPerSecond=" + getIOHitsPerSecond() +
      ", ioTimePerHit=" + getIOTimePerHit();
  }

  public void ioHit(long time) {
    ioHitCount.increment();
    ioHitTime.add(time);
  }

  public void rowCache(long time) {
    rowCacheTime.add(time);
  }

  public void bucketCache(long time) {
    bucketCacheTime.add(time);
  }

  public void cacheBlock(long time) {
    cacheBlockTime.add(time);
  }

  public void checkTime(long time) {
    checkTime.add(time);
  }

  public void cacheTime(long time) {
    cacheTime.add(time);
  }

  public void skipTime(long time) {
    skipTime.add(time);
  }

  public void cellTime(long time) {
    cellTime.add(time);
  }

  public long getrowCacheTime() {
    long time = rowCacheTime.sum() / NANO_TIME;
    return time;
  }
  
  public long getbucketCacheTime() {
    long time = bucketCacheTime.sum() / NANO_TIME;
    return time;
  }

  public long getcacheBlockTime() { 
    long time = cacheBlockTime.sum() / NANO_TIME;
    return time;
  }

  public long getcheckTime() {
    long time = checkTime.sum() / NANO_TIME;
    return time;
  }

  public long getcacheTime() {
    long time = cacheTime.sum() / NANO_TIME;
    return time;
  }

  public long getskipTime() {
    long time = skipTime.sum() / NANO_TIME;
    return time;
  }

  public long getcellTime() {
    long time = cellTime.sum() / NANO_TIME;
    return time;
  }

  public long getIOHitsPerSecond() {
    long now = EnvironmentEdgeManager.currentTime();
    long took = (now - lastLogTime) / 1000;
    lastLogTime = now;
    return took == 0 ? 0 : ioHitCount.sum() / took;
  }

  public double getIOTimePerHit() {
    long time = ioHitTime.sum() / NANO_TIME;
    long count = ioHitCount.sum();
    return ((float) time / (float) count);
  }

  public void reset() {
    ioHitCount.reset();
    ioHitTime.reset();
  }
}
