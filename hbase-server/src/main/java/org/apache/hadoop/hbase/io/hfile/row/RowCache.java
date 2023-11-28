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

import static org.apache.hadoop.hbase.KeyValue.COLUMN_FAMILY_DELIMITER;
import static org.apache.hadoop.hbase.KeyValue.COLUMN_FAMILY_DELIM_ARRAY;
import static org.apache.hadoop.hbase.KeyValue.getDelimiter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockCacheUtil;
import org.apache.hadoop.hbase.io.hfile.BlockPriority;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.RefCnt;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.IdReadWriteLock;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BucketCacheProtos;


@InterfaceAudience.Private
public class RowCache implements HeapSize {
  private static final Logger LOG = LoggerFactory.getLogger(RowCache.class);

  class OffList implements Comparable<OffList> {
   private long off;
   private long frag;

   public OffList(long off, long frag) {
    this.off = off;
    this.frag = frag;
   }

   @Override
   public int compareTo(OffList l) {
     return this.frag <= l.frag ? 1 : -1;
   }
  }

  class KeyEntry {
    private String key;
    private int frequency;
    private int vLength;
    private long timeStamp;
  
    public KeyEntry(String key, int frequency, int vLen, long timeStamp) {
      this.key = key;
      this.frequency = frequency;
      this.vLength = vLen;
      this.timeStamp = timeStamp;
    }

    public String getKey() {
      return this.key;
    }

    public int getFreq() {
      return this.frequency;
    }

    public int getLength() {
      return this.vLength;
    }

    public long getTimeStamp() {
      return this.timeStamp;
    }

    public void update() {
      this.frequency++;
      this.timeStamp = System.nanoTime();
    }
  }

  class BlockEntry implements Comparable<BlockEntry> {
    private BlockCacheKey blockCacheKey;
    public double heapValue;
    transient HashMap<String, KeyEntry> keyMap;

    public BlockEntry(BlockCacheKey blockCacheKey) {
      this.blockCacheKey = blockCacheKey;
      this.keyMap = new HashMap<>();
      heapValue = 10000; //Initialize value as maximum value
    }

    void insert (KeyEntry keyEntry) {
      String rowS = new String(keyEntry.getKey());
      keyMap.put(rowS, keyEntry);
    }

    void update (String row) {
      KeyEntry entry = keyMap.get(row);
      entry.update();
    }

    public BlockCacheKey getKey() {
      return blockCacheKey;
    }

    public double getHeapValue() {
      return heapValue;
    }

    @Override
    public int compareTo(BlockEntry be) {
      return this.heapValue <= be.heapValue ? 1 : -1;
    }
  }
/*
  RowCache Class Variable
*/
  transient final IOEngine ioEngine;
  transient HashMap<String, RowEntry> rowMap;
  transient HashMap<BlockCacheKey, BlockEntry> blockMap;
  transient PriorityQueue<BlockEntry> minHeap;
  static final float minFactor = 0.8f;
  private volatile boolean freeInProgress = false;
  private transient final Lock freeSpaceLock = new ReentrantLock();

  private ArrayList<OffList> offList;
  private BucketCache bucketCache;
  private long baseOffset;
  private long availableSpace;
  private long maximumSpace;
  private long accessCount = 0;
  private long hitCount = 0;
  private long missCount = 0;
  private final int keySize = 20; //Normally, Key size's avg is 20Bytes
  private volatile boolean cacheEnabled;
  private final LongAdder heapSize = new LongAdder();

  public RowCache(String ioEngineName, long capacity) throws IOException {
   
   this.ioEngine = new ByteBufferIOEngine(capacity/4);
   this.baseOffset = 0;
   this.availableSpace = 104857600;
   this.maximumSpace = 1048576000;

   this.offList = new ArrayList<>(); // Initialize offset arraylist
   offList.add(new OffList(baseOffset, availableSpace)); 
   this.rowMap = new HashMap<>(); // Initialize row hashmap
   this.blockMap = new HashMap<>();
   this.minHeap = new PriorityQueue<>();
  }

  public void setBucketCache(BucketCache bucketCache) {
    this.bucketCache = bucketCache;
  } 

  public void cacheRow(Cell cell, BlockCacheKey blockKey) throws IOException {
    RowEntry rowEntry = null;
    byte[] row = copyRowTo(cell);
    byte[] key = copyKeyTo(cell);
    String rowS = new String(row);
    
    if (heapSize() + cell.getValueLength() > maximumSpace) {
      evict();
    } else if (heapSize() + cell.getValueLength() > availableSpace) {
      bucketCache.memoryTrade(5, true); // Can change parameter.
    }

    for (int idx = 0; idx < offList.size(); idx++) {
      OffList list = offList.get(idx);
      if (list.frag >= cell.getValueLength()) {
        rowEntry = new RowEntry(list.off, cell.getRowLength(), cell.getFamilyLength(), 
			cell.getQualifierLength(), cell.getValueLength(), 1, false, blockKey);
	rowEntry.setKey(key);
	list.off = list.off + cell.getValueLength();
	list.frag = list.frag - cell.getValueLength();
	offList.set(idx, list);
        break;
      }
    }
    if (rowEntry == null)
      return; // caching fail caused by off heap memory deficiency.

    rowMap.put(rowS, rowEntry); // Put cell as key, rowEntry as value in rowMap
    BlockEntry entry = blockMap.get(blockKey);

    if (entry != null) {
      KeyEntry newkeyEntry = new KeyEntry(rowS, 1, cell.getValueLength(), System.nanoTime());
      entry.insert(newkeyEntry);
    } else {
      BlockEntry newEntry = new BlockEntry(blockKey);
      blockMap.put(blockKey, newEntry);
      KeyEntry newkeyEntry = new KeyEntry(rowS, 1, cell.getValueLength(), System.nanoTime());
      newEntry.insert(newkeyEntry);
    }
      
    if (cell.getValueLength() > 0 ) {
      byte[] output = new byte[cell.getValueLength()];
      CellUtil.copyValueTo(cell, output, 0);
      ByteBuffer sliceBuf = ByteBuffer.allocate(cell.getValueLength());
      sliceBuf = ByteBuffer.wrap(output);
      sliceBuf.rewind();
      ioEngine.write(sliceBuf, rowEntry.getOffset()); // Write in IOEngine (Offheap memory)
      heapSize.add(cell.getValueLength());
    }
  }


  public Cell getCell(byte[] row) throws IOException {
    String rowS = new String(row);
    RowEntry entry = rowMap.get(rowS);
    accessCount++;

    if (accessCount % 10000 == 0) {
      logStats();
    }

    try {
      byte[] row_ = new byte[entry.getRowLen()];
      byte[] family = new byte[entry.getFamilyLen()];
      byte[] qualifier = null;
      if (entry.getQualifierLen() > 0) {
        qualifier = new byte[entry.getQualifierLen()]; 
      } else {
	qualifier = new byte[1];
      }
      
      byte[] key = null;

      ByteBuffer[] retVal = null;
      String retString = null;
      if (entry != null) {
        retVal = ioEngine.read(entry.offset(), entry.getLength());
        key = entry.getKey();

        for (int i = 0; i < retVal.length; i++) {
          String temp = StandardCharsets.UTF_8.decode(retVal[i]).toString();
          if (retString == null)
	    retString = temp;
          else
            retString = retString + temp;
        }
        System.arraycopy(key, 0, row_, 0, entry.getRowLen());
        System.arraycopy(key, entry.getRowLen(), family, 0, entry.getFamilyLen());
        System.arraycopy(key, entry.getRowLen() + entry.getFamilyLen(), qualifier, 0, entry.getQualifierLen());
        byte[] value = retString.getBytes(StandardCharsets.UTF_8);
        Cell retCell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(row_).setFamily(family)
	        .setQualifier(qualifier).setType(Cell.Type.Put).setValue(value).build();
	
	BlockCacheKey bKey = entry.getBlockKey();
	if (blockMap.containsKey(bKey)) {
	  BlockEntry bEntry = blockMap.get(bKey);
	  bEntry.update(rowS);
	}
        hitCount++;
        return retCell; // return Value as String format
      }
    } catch (NullPointerException ne) { 
	missCount++;
	return null; 
    }
    return null;
  }

  public void evict() {
    if (!freeSpaceLock.tryLock()) {
      return;
    }
    if (rowMap.isEmpty())
      return;
    try {
      freeInProgress = true;
      long bytesToFree = (long) Math.floor(availableSpace * (1 - minFactor));
      long freeBytes = 0;
      long currTime = System.nanoTime();
      // Update Min Heap Value for each entry.
      for (Map.Entry<BlockCacheKey, BlockEntry> entry : blockMap.entrySet()) {
        if (entry.getValue().keyMap.size() > 0) {
	  double tmpValue = 0;
	  for (Map.Entry<String, KeyEntry> keyEntry : entry.getValue().keyMap.entrySet()) {
	     tmpValue += ((double)keyEntry.getValue().getFreq() / (double)keyEntry.getValue().getLength()) 
	  	     / (double)keyEntry.getValue().getTimeStamp() / (double) currTime;
	  }
	  entry.getValue().heapValue = tmpValue / entry.getValue().keyMap.size();
        }
        minHeap.add(entry.getValue());
      } 

      while (bytesToFree > freeBytes) {
        BlockEntry removeEntry = minHeap.poll(); // Remove from Min Heap (Queue).
        for (Map.Entry<String, KeyEntry> keyEntry : removeEntry.keyMap.entrySet()) {
          freeBytes += keyEntry.getValue().getLength();
	  RowEntry entry = rowMap.get(keyEntry.getKey());
	  offList.add(new OffList(entry.offset(), entry.getLength()));
	  Collections.sort(offList);
	  rowMap.remove(keyEntry.getKey()); // Remove from rowMap (HashMap).
        }
        removeEntry.keyMap.clear(); // Remove from keyMap (HashMap);
        blockMap.remove(removeEntry.getKey()); // Remove from BlockEntry (HashMap).
      }
 
      heapSize.add(-1 * freeBytes);
  
    } catch (Throwable t) {
      LOG.warn("Failed freeing space", t);
    } finally {
      freeInProgress = false;
      freeSpaceLock.unlock();
    }
  }

  public void getMemory(long additionalCapacity, long baseOffset) {
    availableSpace = availableSpace + additionalCapacity;
    offList.add(new OffList(heapSize()+1, additionalCapacity));
    Collections.sort(offList);
  }

  public static byte[] copyKeyTo(Cell cell) {
    int rowLen = (int)cell.getRowLength();
    int flen = (int)cell.getFamilyLength();
    int qlen = cell.getQualifierLength();

    byte[] key = new byte[rowLen+flen+qlen];

    CellUtil.copyRowTo(cell, key, 0);
    CellUtil.copyFamilyTo(cell, key, rowLen);
    CellUtil.copyQualifierTo(cell, key, rowLen + flen);
    return key;
  }

  public static byte[] copyRowTo(Cell cell) {
    int rowLen = (int)cell.getRowLength();
    byte[] row = new byte[rowLen];

    CellUtil.copyRowTo(cell, row, 0);
    return row;
  }

  @Override
  public long heapSize() {
    return this.heapSize.sum();
  }

  public double getHitRatio() {
    return (double)hitCount / (double)accessCount;
  }

  public void logStats() {
    LOG.info("RowCache AccessCount=" + accessCount + ", " +
	"RowCache HitCount=" + hitCount + ", " +
	"RowCache MissCount=" + missCount + ", " +
	"RowCache HitRatio=" + getHitRatio()*100 + "%, " +
	"RowCache heapSize=" + heapSize() + ", " +
	"RowCache AvailableSpace=" + availableSpace + ", " +
	"RowCache KV Size=" + rowMap.size());
  }
}


