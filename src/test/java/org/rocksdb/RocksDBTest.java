// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import junit.framework.TestCase;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class RocksDBTest extends TestCase {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();



  File getTestDirectory(String name) {
    File rc = new File(new File("test-data"), name);
    rc.mkdirs();
    return rc;
  }

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void testOpen() throws RocksDBException {
    try (final RocksDB db =
             RocksDB.open(getTestDirectory(getName()).getAbsolutePath())) {
      assertNotNull(db);
    }
  }

  @Test
  public void testOpen_opt() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             getTestDirectory(getName()).getAbsolutePath())) {
      assertNotNull(db);
    }
  }

  @Test
  public void testOpenWhenOpen() throws RocksDBException {
    final String dbPath = getTestDirectory(getName()).getAbsolutePath();

    try (final RocksDB db1 = RocksDB.open(dbPath)) {
      try (final RocksDB db2 = RocksDB.open(dbPath)) {
        fail("Should have thrown an exception when opening the same db twice");
      } catch (final RocksDBException e) {
        assertEquals(e.getStatus().getCode(),Status.Code.IOError);
        assertEquals(e.getStatus().getSubCode(),(Status.SubCode.None));
        assertTrue(e.getStatus().getState().contains("lock "));
      }
    }
  }

  @Test
  public void testPut() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(getTestDirectory(getName()).getAbsolutePath());
         final WriteOptions opt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put(opt, "key2".getBytes(), "12345678".getBytes());
      assertEquals(new String(db.get("key1".getBytes())),
          "value");
      assertEquals(new String(db.get("key2".getBytes())),
          "12345678");
    }
  }

  @Test
  public void testWrite() throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options options = new Options()
             .setMergeOperator(stringAppendOperator)
             .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             getTestDirectory(getName()).getAbsolutePath());
         final WriteOptions opts = new WriteOptions()) {

      try (final WriteBatch wb1 = new WriteBatch()) {
        wb1.put("key1".getBytes(), "aa".getBytes());
        wb1.merge("key1".getBytes(), "bb".getBytes());

        try (final WriteBatch wb2 = new WriteBatch()) {
          wb2.put("key2".getBytes(), "xx".getBytes());
          wb2.merge("key2".getBytes(), "yy".getBytes());
          db.write(opts, wb1);
          db.write(opts, wb2);
        }
      }

      assertEquals(new String(db.get("key1".getBytes())),
          "aa,bb");
      assertEquals(new String(db.get("key2".getBytes())),
          "xx,yy");
    }
  }

  @Test
  public void testGetWithOutValue() throws RocksDBException {
    try (final RocksDB db =
             RocksDB.open(getTestDirectory(getName()).getAbsolutePath())) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      byte[] outValue = new byte[5];
      // not found value
      int getResult = db.get("keyNotFound".getBytes(), outValue);
      assertEquals(getResult,RocksDB.NOT_FOUND);
      // found value which fits in outValue
      getResult = db.get("key1".getBytes(), outValue);
      assertNotEquals(getResult,RocksDB.NOT_FOUND);
      assertEquals(new String(outValue),"value");
      // found value which fits partially
      getResult = db.get("key2".getBytes(), outValue);
      assertNotEquals(getResult,RocksDB.NOT_FOUND);
      assertEquals(new String(outValue),"12345");
    }
  }

  @Test
  public void testGetWithOutValueReadOptions() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(getTestDirectory(getName()).getAbsolutePath());
         final ReadOptions rOpt = new ReadOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      byte[] outValue = new byte[5];
      // not found value
      int getResult = db.get(rOpt, "keyNotFound".getBytes(),
          outValue);
      assertEquals(getResult,RocksDB.NOT_FOUND);
      // found value which fits in outValue
      getResult = db.get(rOpt, "key1".getBytes(), outValue);
      assertNotEquals(getResult,RocksDB.NOT_FOUND);
      assertEquals(new String(outValue),"value");
      // found value which fits partially
      getResult = db.get(rOpt, "key2".getBytes(), outValue);
      assertNotEquals(getResult,RocksDB.NOT_FOUND);
      assertEquals(new String(outValue),"12345");
    }
  }


  @Test
  public void testGetOutOfArrayMaxSizeValue() {
    final int numberOfValueSplits = 10;
    final int splitSize = Integer.MAX_VALUE / numberOfValueSplits;

    Runtime runtime = Runtime.getRuntime();
    long neededMemory = ((long)(splitSize)) * (((long)numberOfValueSplits) + 3);
    boolean isEnoughMemory = runtime.maxMemory() - runtime.totalMemory() > neededMemory;
    Assume.assumeTrue(isEnoughMemory);

    final byte[] valueSplit = new byte[splitSize];
    final byte[] key = "key".getBytes();

    // merge (numberOfValueSplits + 1) valueSplit's to get value size exceeding Integer.MAX_VALUE
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options opt = new Options()
             .setCreateIfMissing(true)
             .setMergeOperator(stringAppendOperator);
         final RocksDB db = RocksDB.open(opt, getTestDirectory(getName()).getAbsolutePath())) {
      db.put(key, valueSplit);
      for (int i = 0; i < numberOfValueSplits; i++) {
        db.merge(key, valueSplit);
      }
      db.get(key);
    } catch (RocksDBException exception) {
      assertEquals("Requested array size exceeds VM limit",exception.getMessage());
    }
  }

  @Test
  public void testMultiGet() throws RocksDBException, InterruptedException {
    try (final RocksDB db = RocksDB.open(getTestDirectory(getName()).getAbsolutePath());
         final ReadOptions rOpt = new ReadOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      List<byte[]> lookupKeys = new ArrayList<>();
      lookupKeys.add("key1".getBytes());
      lookupKeys.add("key2".getBytes());
      Map<byte[], byte[]> results = db.multiGet(lookupKeys);
      Map<String,String> m = results.entrySet().stream().collect(Collectors.toMap(
          e-> new String(e.getKey()),e-> new String(e.getValue())));
      assertNotNull(results);
      assertNotNull(results.values());
      assertTrue(m.containsValue("value"));
      assertTrue(m.containsValue( "12345678"));
      // test same method with ReadOptions
      results = db.multiGet(rOpt, lookupKeys);
      m = results.entrySet().stream().collect(Collectors.toMap(
          e-> new String(e.getKey()),e-> new String(e.getValue())));
      assertNotNull(results);
      assertNotNull(results.values());
      assertTrue(m.containsValue("value"));
      assertTrue(m.containsValue( "12345678"));

      // remove existing key
      lookupKeys.remove("key2".getBytes());
      // add non existing key
      lookupKeys.add("key3".getBytes());
      results = db.multiGet(lookupKeys);
      m = results.entrySet().stream().collect(Collectors.toMap(
          e-> new String(e.getKey()),e-> new String(e.getValue())));
      assertNotNull(results);
      assertNotNull(results.values());
      assertTrue(m.containsValue("value"));
      // test same call with readOptions
      results = db.multiGet(rOpt, lookupKeys);
      assertNotNull(results);
      assertNotNull(results.values());
      assertTrue(m.containsValue("value"));
    }
  }

  @Test
  public void testMerge() throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options opt = new Options()
             .setCreateIfMissing(true)
             .setMergeOperator(stringAppendOperator);
         final WriteOptions wOpt = new WriteOptions();
         final RocksDB db = RocksDB.open(opt,
             getTestDirectory(getName()).getAbsolutePath())
    ) {
      db.put("key1".getBytes(), "value".getBytes());
      assertEquals(new String(db.get("key1".getBytes())),
          "value");
      // merge key1 with another value portion
      db.merge("key1".getBytes(), "value2".getBytes());
      assertEquals(new String(db.get("key1".getBytes())),
          "value,value2");
      // merge key1 with another value portion
      db.merge(wOpt, "key1".getBytes(), "value3".getBytes());
      assertEquals(new String(db.get("key1".getBytes())),
          "value,value2,value3");
      // merge on non existent key shall insert the value
      db.merge(wOpt, "key2".getBytes(), "xxxx".getBytes());
      assertEquals(new String(db.get("key2".getBytes())),
          "xxxx");
    }
  }

  @Test
  public void testDelete() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(getTestDirectory(getName()).getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      assertEquals(new String(db.get("key1".getBytes())),
          "value");
      assertEquals(new String(db.get("key2".getBytes())),
          "12345678");
      db.delete("key1".getBytes());
      db.delete(wOpt, "key2".getBytes());
      assertNull(db.get("key1".getBytes()));
      assertNull(db.get("key2".getBytes()));
    }
  }

  @Test
  public void testSingleDelete() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(getTestDirectory(getName()).getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      assertEquals(new String(db.get("key1".getBytes())),
          "value");
      assertEquals(new String(db.get("key2".getBytes())),
          "12345678");
      db.singleDelete("key1".getBytes());
      db.singleDelete(wOpt, "key2".getBytes());
      assertNull(db.get("key1".getBytes()));
      assertNull(db.get("key2".getBytes()));
    }
  }

  @Test
  public void testSingleDelete_nonExisting() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(getTestDirectory(getName()).getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.singleDelete("key1".getBytes());
      db.singleDelete(wOpt, "key2".getBytes());
      assertNull(db.get("key1".getBytes()));
      assertNull(db.get("key2".getBytes()));
    }
  }

  @Test
  public void testDeleteRange() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(getTestDirectory(getName()).getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      db.put("key3".getBytes(), "abcdefg".getBytes());
      db.put("key4".getBytes(), "xyz".getBytes());
      assertEquals(new String(db.get("key1".getBytes())),"value");
      assertEquals(new String(db.get("key2".getBytes())),"12345678");
      assertEquals(new String(db.get("key3".getBytes())),"abcdefg");
      assertEquals(new String(db.get("key4".getBytes())),"xyz");
      db.deleteRange("key2".getBytes(), "key4".getBytes());
      assertEquals(new String(db.get("key1".getBytes())),"value");
      assertNull(db.get("key2".getBytes()));
      assertNull(db.get("key3".getBytes()));
      assertEquals(new String(db.get("key4".getBytes())),"xyz");
    }
  }

  @Test
  public void testGetIntProperty() throws RocksDBException {
    try (
        final Options options = new Options()
            .setCreateIfMissing(true)
            .setMaxWriteBufferNumber(10)
            .setMinWriteBufferNumberToMerge(10);
        final RocksDB db = RocksDB.open(options,
            getTestDirectory(getName()).getAbsolutePath());
        final WriteOptions wOpt = new WriteOptions().setDisableWAL(true)
    ) {
      db.put(wOpt, "key1".getBytes(), "value1".getBytes());
      db.put(wOpt, "key2".getBytes(), "value2".getBytes());
      db.put(wOpt, "key3".getBytes(), "value3".getBytes());
      db.put(wOpt, "key4".getBytes(), "value4".getBytes());
      assertTrue(db.getLongProperty("rocksdb.num-entries-active-mem-table") > 0)
      ;
      assertTrue(db.getLongProperty("rocksdb.cur-size-active-mem-table") > 0);
    }
  }

  @Test
  public void testFullCompactRange() throws RocksDBException {
    try (final Options opt = new Options().
        setCreateIfMissing(true).
        setDisableAutoCompactions(true).
        setCompactionStyle(CompactionStyle.LEVEL).
        setNumLevels(4).
        setWriteBufferSize(100 << 10).
        setLevelZeroFileNumCompactionTrigger(3).
        setTargetFileSizeBase(200 << 10).
        setTargetFileSizeMultiplier(1).
        setMaxBytesForLevelBase(500 << 10).
        setMaxBytesForLevelMultiplier(1).
        setDisableAutoCompactions(false);
         final RocksDB db = RocksDB.open(opt,
             getTestDirectory(getName()).getAbsolutePath())) {
      // fill database with key/value pairs
      byte[] b = new byte[10000];
      for (int i = 0; i < 200; i++) {
        rand.nextBytes(b);
        db.put((String.valueOf(i)).getBytes(), b);
      }
      db.compactRange();
    }
  }

  @Test
  public void testFullCompactRangeColumnFamily()
      throws RocksDBException {
    try (
        final DBOptions opt = new DBOptions().
            setCreateIfMissing(true).
            setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
            setDisableAutoCompactions(true).
            setCompactionStyle(CompactionStyle.LEVEL).
            setNumLevels(4).
            setWriteBufferSize(100 << 10).
            setLevelZeroFileNumCompactionTrigger(3).
            setTargetFileSizeBase(200 << 10).
            setTargetFileSizeMultiplier(1).
            setMaxBytesForLevelBase(500 << 10).
            setMaxBytesForLevelMultiplier(1).
            setDisableAutoCompactions(false)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts));

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      try (final RocksDB db = RocksDB.open(opt,
          getTestDirectory(getName()).getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          byte[] b = new byte[10000];
          for (int i = 0; i < 200; i++) {
            rand.nextBytes(b);
            db.put(columnFamilyHandles.get(1),
                String.valueOf(i).getBytes(), b);
          }
          db.compactRange(columnFamilyHandles.get(1));
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void testCompactRangeWithKeys()
      throws RocksDBException {
    try (final Options opt = new Options().
        setCreateIfMissing(true).
        setDisableAutoCompactions(true).
        setCompactionStyle(CompactionStyle.LEVEL).
        setNumLevels(4).
        setWriteBufferSize(100 << 10).
        setLevelZeroFileNumCompactionTrigger(3).
        setTargetFileSizeBase(200 << 10).
        setTargetFileSizeMultiplier(1).
        setMaxBytesForLevelBase(500 << 10).
        setMaxBytesForLevelMultiplier(1).
        setDisableAutoCompactions(false);
         final RocksDB db = RocksDB.open(opt,
             getTestDirectory(getName()).getAbsolutePath())) {
      // fill database with key/value pairs
      byte[] b = new byte[10000];
      for (int i = 0; i < 200; i++) {
        rand.nextBytes(b);
        db.put((String.valueOf(i)).getBytes(), b);
      }
      db.compactRange("0".getBytes(), "201".getBytes());
    }
  }

  @Test
  public void testCompactRangeWithKeysReduce()
      throws RocksDBException {
    try (
        final Options opt = new Options().
            setCreateIfMissing(true).
            setDisableAutoCompactions(true).
            setCompactionStyle(CompactionStyle.LEVEL).
            setNumLevels(4).
            setWriteBufferSize(100 << 10).
            setLevelZeroFileNumCompactionTrigger(3).
            setTargetFileSizeBase(200 << 10).
            setTargetFileSizeMultiplier(1).
            setMaxBytesForLevelBase(500 << 10).
            setMaxBytesForLevelMultiplier(1).
            setDisableAutoCompactions(false);
        final RocksDB db = RocksDB.open(opt,
            getTestDirectory(getName()).getAbsolutePath())) {
      // fill database with key/value pairs
      byte[] b = new byte[10000];
      for (int i = 0; i < 200; i++) {
        rand.nextBytes(b);
        db.put((String.valueOf(i)).getBytes(), b);
      }
      db.flush(new FlushOptions().setWaitForFlush(true));
      db.compactRange("0".getBytes(), "201".getBytes(),
          true, -1, 0);
    }
  }

  @Test
  public void testCompactRangeWithKeysColumnFamily()
      throws RocksDBException {
    try (final DBOptions opt = new DBOptions().
        setCreateIfMissing(true).
        setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
             setDisableAutoCompactions(true).
             setCompactionStyle(CompactionStyle.LEVEL).
             setNumLevels(4).
             setWriteBufferSize(100 << 10).
             setLevelZeroFileNumCompactionTrigger(3).
             setTargetFileSizeBase(200 << 10).
             setTargetFileSizeMultiplier(1).
             setMaxBytesForLevelBase(500 << 10).
             setMaxBytesForLevelMultiplier(1).
             setDisableAutoCompactions(false)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles =
          new ArrayList<>();
      try (final RocksDB db = RocksDB.open(opt,
          getTestDirectory(getName()).getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          byte[] b = new byte[10000];
          for (int i = 0; i < 200; i++) {
            rand.nextBytes(b);
            db.put(columnFamilyHandles.get(1),
                String.valueOf(i).getBytes(), b);
          }
          db.compactRange(columnFamilyHandles.get(1),
              "0".getBytes(), "201".getBytes());
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void testCompactRangeWithKeysReduceColumnFamily()
      throws RocksDBException {
    try (final DBOptions opt = new DBOptions().
        setCreateIfMissing(true).
        setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
             setDisableAutoCompactions(true).
             setCompactionStyle(CompactionStyle.LEVEL).
             setNumLevels(4).
             setWriteBufferSize(100 << 10).
             setLevelZeroFileNumCompactionTrigger(3).
             setTargetFileSizeBase(200 << 10).
             setTargetFileSizeMultiplier(1).
             setMaxBytesForLevelBase(500 << 10).
             setMaxBytesForLevelMultiplier(1).
             setDisableAutoCompactions(false)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      // open database
      try (final RocksDB db = RocksDB.open(opt,
          getTestDirectory(getName()).getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          byte[] b = new byte[10000];
          for (int i = 0; i < 200; i++) {
            rand.nextBytes(b);
            db.put(columnFamilyHandles.get(1),
                String.valueOf(i).getBytes(), b);
          }
          db.compactRange(columnFamilyHandles.get(1), "0".getBytes(),
              "201".getBytes(), true, -1, 0);
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void testCompactRangeToLevel()
      throws RocksDBException, InterruptedException {
    final int NUM_KEYS_PER_L0_FILE = 100;
    final int KEY_SIZE = 20;
    final int VALUE_SIZE = 300;
    final int L0_FILE_SIZE =
        NUM_KEYS_PER_L0_FILE * (KEY_SIZE + VALUE_SIZE);
    final int NUM_L0_FILES = 10;
    final int TEST_SCALE = 5;
    final int KEY_INTERVAL = 100;
    try (final Options opt = new Options().
        setCreateIfMissing(true).
        setCompactionStyle(CompactionStyle.LEVEL).
        setNumLevels(5).
        // a slightly bigger write buffer than L0 file
        // so that we can ensure manual flush always
        // go before background flush happens.
            setWriteBufferSize(L0_FILE_SIZE * 2).
        // Disable auto L0 -> L1 compaction
            setLevelZeroFileNumCompactionTrigger(20).
            setTargetFileSizeBase(L0_FILE_SIZE * 100).
            setTargetFileSizeMultiplier(1).
        // To disable auto compaction
            setMaxBytesForLevelBase(NUM_L0_FILES * L0_FILE_SIZE * 100).
            setMaxBytesForLevelMultiplier(2).
            setDisableAutoCompactions(true);
         final RocksDB db = RocksDB.open(opt,
             getTestDirectory(getName()).getAbsolutePath())
    ) {
      // fill database with key/value pairs
      byte[] value = new byte[VALUE_SIZE];
      int int_key = 0;
      for (int round = 0; round < 5; ++round) {
        int initial_key = int_key;
        for (int f = 1; f <= NUM_L0_FILES; ++f) {
          for (int i = 0; i < NUM_KEYS_PER_L0_FILE; ++i) {
            int_key += KEY_INTERVAL;
            rand.nextBytes(value);

            db.put(String.format("%020d", int_key).getBytes(),
                value);
          }
          db.flush(new FlushOptions().setWaitForFlush(true));
          // Make sure we do create one more L0 files.
          assertEquals(
              db.getProperty("rocksdb.num-files-at-level0"),"" + f);
        }

        // Compact all L0 files we just created
        db.compactRange(
            String.format("%020d", initial_key).getBytes(),
            String.format("%020d", int_key - 1).getBytes());
        // Making sure there isn't any L0 files.
        assertEquals(
            db.getProperty("rocksdb.num-files-at-level0"),"0");
        // Making sure there are some L1 files.
        // Here we only use != 0 instead of a specific number
        // as we don't want the test make any assumption on
        // how compaction works.
        assertNotEquals(
            db.getProperty("rocksdb.num-files-at-level1"),"0");
        // Because we only compacted those keys we issued
        // in this round, there shouldn't be any L1 -> L2
        // compaction.  So we expect zero L2 files here.
        assertEquals(
            db.getProperty("rocksdb.num-files-at-level2"),"0");
      }
    }
  }

  @Test
  public void testCompactRangeToLevelColumnFamily()
      throws RocksDBException {
    final int NUM_KEYS_PER_L0_FILE = 100;
    final int KEY_SIZE = 20;
    final int VALUE_SIZE = 300;
    final int L0_FILE_SIZE =
        NUM_KEYS_PER_L0_FILE * (KEY_SIZE + VALUE_SIZE);
    final int NUM_L0_FILES = 10;
    final int TEST_SCALE = 5;
    final int KEY_INTERVAL = 100;

    try (final DBOptions opt = new DBOptions().
        setCreateIfMissing(true).
        setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
             setCompactionStyle(CompactionStyle.LEVEL).
             setNumLevels(5).
             // a slightly bigger write buffer than L0 file
             // so that we can ensure manual flush always
             // go before background flush happens.
                 setWriteBufferSize(L0_FILE_SIZE * 2).
             // Disable auto L0 -> L1 compaction
                 setLevelZeroFileNumCompactionTrigger(20).
                 setTargetFileSizeBase(L0_FILE_SIZE * 100).
                 setTargetFileSizeMultiplier(1).
             // To disable auto compaction
                 setMaxBytesForLevelBase(NUM_L0_FILES * L0_FILE_SIZE * 100).
                 setMaxBytesForLevelMultiplier(2).
                 setDisableAutoCompactions(true)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      // open database
      try (final RocksDB db = RocksDB.open(opt,
          getTestDirectory(getName()).getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          byte[] value = new byte[VALUE_SIZE];
          int int_key = 0;
          for (int round = 0; round < 5; ++round) {
            int initial_key = int_key;
            for (int f = 1; f <= NUM_L0_FILES; ++f) {
              for (int i = 0; i < NUM_KEYS_PER_L0_FILE; ++i) {
                int_key += KEY_INTERVAL;
                rand.nextBytes(value);

                db.put(columnFamilyHandles.get(1),
                    String.format("%020d", int_key).getBytes(),
                    value);
              }
              db.flush(new FlushOptions().setWaitForFlush(true),
                  columnFamilyHandles.get(1));
              // Make sure we do create one more L0 files.
              assertEquals(
                  db.getProperty(columnFamilyHandles.get(1),
                      "rocksdb.num-files-at-level0"),"" + f);
            }

            // Compact all L0 files we just created
            db.compactRange(
                columnFamilyHandles.get(1),
                String.format("%020d", initial_key).getBytes(),
                String.format("%020d", int_key - 1).getBytes());
            // Making sure there isn't any L0 files.
            assertEquals(
                db.getProperty(columnFamilyHandles.get(1),
                    "rocksdb.num-files-at-level0"),"0");
            // Making sure there are some L1 files.
            // Here we only use != 0 instead of a specific number
            // as we don't want the test make any assumption on
            // how compaction works.
            assertNotEquals(
                db.getProperty(columnFamilyHandles.get(1),
                    "rocksdb.num-files-at-level1"),"0");
            // Because we only compacted those keys we issued
            // in this round, there shouldn't be any L1 -> L2
            // compaction.  So we expect zero L2 files here.
            assertEquals(
                db.getProperty(columnFamilyHandles.get(1),
                    "rocksdb.num-files-at-level2"),"0");
          }
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void testPauseContinueBackgroundWork() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             getTestDirectory(getName()).getAbsolutePath())
    ) {
      db.pauseBackgroundWork();
      db.continueBackgroundWork();
      db.pauseBackgroundWork();
      db.continueBackgroundWork();
    }
  }

  @Test
  public void testEnableDisableFileDeletions() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             getTestDirectory(getName()).getAbsolutePath())
    ) {
      db.disableFileDeletions();
      db.enableFileDeletions(false);
      db.disableFileDeletions();
      db.enableFileDeletions(true);
    }
  }

  @Test
  public void testSetOptions() throws RocksDBException {
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions()
             .setWriteBufferSize(4096)) {

      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts));

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      try (final RocksDB db = RocksDB.open(options,
          getTestDirectory(getName()).getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles)) {
        try {
          final MutableColumnFamilyOptions mutableOptions =
              MutableColumnFamilyOptions.builder()
                  .setWriteBufferSize(2048)
                  .build();

          db.setOptions(columnFamilyHandles.get(1), mutableOptions);

        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void testDestroyDB() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      String dbPath = getTestDirectory(getName()).getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.put("key1".getBytes(), "value".getBytes());
      }
      assertTrue(new File(dbPath).exists());
      RocksDB.destroyDB(dbPath, options);
      assertFalse(new File(dbPath).exists());
    }
  }

  @Test
  public void testDestroyDBFailIfOpen() {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      String dbPath = getTestDirectory(getName()).getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        // Fails as the db is open and locked.
        RocksDB.destroyDB(dbPath, options);
      } catch (Exception exception) {
        assertTrue(exception instanceof RocksDBException );
      }
    }
  }
}
