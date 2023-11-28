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

package org.apache.hadoop.hbase.io.hfile.row;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.hadoop.hbase.io.hfile.BlockPriority;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializerIdManager;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.HBaseReferenceCounted;
import org.apache.hadoop.hbase.nio.RefCnt;
import org.apache.hadoop.hbase.util.IdReadWriteLock;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class RowEntry implements HBaseReferenceCounted {
  // access counter comparator, descending order
  static final Comparator<RowEntry> COMPARATOR = 
    Comparator.comparingLong(RowEntry::getAccessCounter).reversed();

  private int offsetBase;
  private short rLen;
  private byte fLen;
  private int qLen;
  private int vLen;
  private byte offset1;
  private long offsetOriginal;

  private BlockCacheKey blockCacheKey;

  private byte[] key;

  private volatile long accessCounter;
  final AtomicBoolean markedAsEvicted;
  
  private final long cachedTime = System.nanoTime();

  RowEntry(long offset, short rLen, byte fLen, int qLen, int vLen, long accessCounter, boolean inMemory, BlockCacheKey blockCacheKey) {
    
    setOffset(offset);
    this.offsetOriginal = offset;
    this.rLen = rLen;
    this.fLen = fLen;
    this.qLen = qLen;
    this.vLen = vLen;
    this.accessCounter = accessCounter;
    this.markedAsEvicted = new AtomicBoolean(false);
    this.blockCacheKey = blockCacheKey;
  }

  long offset() {
    long o = ((long) offsetBase) & 0xFFFFFFFFL;
    o += (((long) (offset1)) & 0XFF) << 32;
    return o << 8;
  }

  private void setOffset(long value) {
    assert (value & 0xFF) == 0;
    value >>= 8;
    offsetBase = (int) value;
    offset1 = (byte) (value >> 32);
  }

  public void setKey(byte[] key) {
    this.key = key;
  }

  public byte[] getKey() {
    return this.key;
  }

  public BlockCacheKey getBlockKey() {
    return blockCacheKey;
  }

  public int getLength() {
    return vLen;
  }

  public short getRowLen() {
    return rLen;
  }

  public byte getFamilyLen() {
    return fLen;
  }

  public int getQualifierLen() {
    return qLen;
  }

  public int getValueLen() {
    return vLen;
  }

  public long getOffset() {
    return offsetOriginal;
  }

  public long getAccessCounter() {
    return accessCounter;
  }

  void access (long accessCounter) {
    this.accessCounter = accessCounter;
  }
 
  long getCachedTime() {
    return cachedTime;
  }

  @Override
  public RowEntry retain() {
  /** Nothing to do */
    return this;
  } 

  @Override 
  public boolean release() {
  /** Nothing to do */
    return true;
  }

  @Override
  public int refCnt() {
  /** Nothing to do */
    return 1;
  }
}


 
