/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractN;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferItems;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;
import static com.yahoo.sketches.quantiles.Util.computeCombinedBufferItemCapacity;
import static com.yahoo.sketches.quantiles.Util.computeRetainedItems;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Implements the DoublesSketch on the Java heap.
 *
 * @author Lee Rhodes
 */
final class HeapDoublesSketch extends DoublesSketch {
  static final int MIN_HEAP_DOUBLES_SER_VER = 1;

  /**
   * The smallest value ever seen in the stream.
   */
  private double minValue_;

  /**
   * The largest value ever seen in the stream.
   */
  private double maxValue_;

  /**
   * The total count of items seen.
   */
  private long n_;

  /**
   * Number of samples currently in base buffer.
   *
   * <p>Count = N % (2*K)
   */
  private int baseBufferCount_;

  /**
   * Active levels expressed as a bit pattern.
   *
   * <p>Pattern = N / (2 * K)
   */
  private long bitPattern_;

  /**
   * In the initial on-heap version, equals combinedBuffer_.length.
   * May differ in later versions that grow space more aggressively.
   * Also, in the off-heap version, combinedBuffer_ won't be a java array,
   * so it won't know its own length.
   */
  private int combinedBufferItemCapacity_;

  /**
   * This single array contains the base buffer plus all levels some of which may not be used,
   * i.e, is in non-compact form.
   * A level is of size K and is either full and sorted, or not used. A "not used" buffer may have
   * garbage. Whether a level buffer used or not is indicated by the bitPattern_.
   * The base buffer has length 2*K but might not be full and isn't necessarily sorted.
   * The base buffer precedes the level buffers. This buffer does not include the min, max values.
   *
   * <p>The levels arrays require quite a bit of explanation, which we defer until later.</p>
   */
  private double[] combinedBuffer_;

  //**CONSTRUCTORS**********************************************************
  private HeapDoublesSketch(final int k) {
    super(k); //Checks k
  }

  /**
   * Obtains a new instance of a DoublesSketch.
   *
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * Must be greater than 1 and less than 65536 and a power of 2.
   * @return a HeapQuantileSketch
   */
  static HeapDoublesSketch newInstance(final int k) {
    final HeapDoublesSketch hqs = new HeapDoublesSketch(k);
    final int bufAlloc = 2 * Math.min(DoublesSketch.MIN_K, k); //the min is important
    hqs.n_ = 0;
    hqs.combinedBufferItemCapacity_ = bufAlloc;
    hqs.combinedBuffer_ = new double[bufAlloc];
    hqs.baseBufferCount_ = 0;
    hqs.bitPattern_ = 0;
    hqs.minValue_ = Double.POSITIVE_INFINITY;
    hqs.maxValue_ = Double.NEGATIVE_INFINITY;
    return hqs;
  }

  /**
   * Heapifies the given srcMem, which must be a Memory image of a DoublesSketch and may have data.
   * @param srcMem a Memory image of a sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a DoublesSketch on the Java heap.
   */
  static HeapDoublesSketch heapifyInstance(final Memory srcMem) {
    final long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new SketchesArgumentException("Source Memory too small: " + memCapBytes + " < 8");
    }
    final Object memObj = srcMem.array(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    //Extract the preamble first 8 bytes
    final int preLongs = extractPreLongs(memObj, memAdd);
    final int serVer = extractSerVer(memObj, memAdd);
    final int familyID = extractFamilyID(memObj, memAdd);
    final int flags = extractFlags(memObj, memAdd);
    final int k = extractK(memObj, memAdd);
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0; //Preamble flags empty state

    //VALIDITY CHECKS
    DoublesUtil.checkDoublesSerVer(serVer, MIN_HEAP_DOUBLES_SER_VER);
    Util.checkHeapFlags(flags);
    checkPreLongsEmpty(preLongs, empty, serVer);
    Util.checkFamilyID(familyID);

    final HeapDoublesSketch hds = newInstance(k); //checks k
    if (empty) { return hds; }

    //Not empty, must have valid preamble + min, max, n.
    //Forward compatibility from SerVer = 1 :
    final boolean srcIsCompact = (serVer == 2) | ((flags & COMPACT_FLAG_MASK) > 0);

    final long n = extractN(memObj, memAdd); //Second 8 bytes of preamble
    checkHeapMemCapacity(k, n, srcIsCompact, serVer, memCapBytes);

    //set class members by computing them
    hds.n_ = n;
    final boolean partialBaseBuffer = true; //only relevant when there are no levels.
    final int combBufCap = computeCombinedBufferItemCapacity(k, n, partialBaseBuffer);
    hds.combinedBufferItemCapacity_ = combBufCap;
    hds.baseBufferCount_ = computeBaseBufferItems(k, n);
    hds.bitPattern_ = computeBitPattern(k, n);
    //Extract min, max, data from srcMem into Combined Buffer
    hds.srcMemoryToCombinedBuffer(srcMem, serVer, srcIsCompact, combBufCap);
    return hds;
  }

  @Override
  public void update(final double dataItem) {
    if (Double.isNaN(dataItem)) { return; }
    final double maxValue = getMaxValue();
    final double minValue = getMinValue();

    if (dataItem > maxValue) { putMaxValue(dataItem); }
    if (dataItem < minValue) { putMinValue(dataItem); }

    //don't increment n_ and baseBufferCount- yet
    final int curBBCount = baseBufferCount_;
    final int newBBCount = curBBCount + 1;
    final long newN = n_ + 1;

    if (newBBCount > combinedBufferItemCapacity_) {
      growBaseBuffer(); //only changes combinedBuffer,  combinedBufferItemCapacity
    }

    //put the new item in the base buffer
    combinedBuffer_[curBBCount] = dataItem;

    if (newBBCount == 2 * k_) { //Propogate

      // make sure there will be enough levels for the propagation
      final int spaceNeeded = DoublesUpdateImpl.maybeGrowLevels(newN, k_);

      if (spaceNeeded > combinedBufferItemCapacity_) {
        // copies base buffer plus old levels, adds space for new level
        growCombinedBuffer(combinedBufferItemCapacity_, spaceNeeded);
      }

      // note that combinedBuffer_ is after the possible resizing above
      Arrays.sort(combinedBuffer_, 0, k_ << 1); //sort only the BB portion, which is full

      final long newBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
          0,               //starting level
          null,            //sizeKbuf,   not needed here
          0,               //sizeKStart, not needed here
          combinedBuffer_, //size2Kbuf, the base buffer = the Combined Buffer, possibly resized
          0,               //size2KStart
          true,            //doUpdateVersion
          k_,
          combinedBuffer_, //the base buffer = the Combined Buffer, possibly resized
          bitPattern_      //current bitPattern prior to updating n
      );
      assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check

      bitPattern_ = newBitPattern;
      baseBufferCount_ = 0;
    } else {
      //bitPattern unchanged
      baseBufferCount_ = newBBCount;
    }
    n_ = newN;
  }

  @Override
  public int getK() {
    return k_;
  }

  @Override
  public long getN() {
    return n_;
  }

  @Override
  public boolean isEmpty() {
    return (n_ == 0);
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public double getMinValue() {
    return minValue_;
  }

  @Override
  public double getMaxValue() {
    return maxValue_;
  }

  @Override
  public void reset() {
    n_ = 0;
    combinedBufferItemCapacity_ = 2 * Math.min(DoublesSketch.MIN_K, k_); //the min is important
    combinedBuffer_ = new double[combinedBufferItemCapacity_];
    baseBufferCount_ = 0;
    bitPattern_ = 0;
    minValue_ = Double.POSITIVE_INFINITY;
    maxValue_ = Double.NEGATIVE_INFINITY;
  }

  /**
   * Loads the Combined Buffer, min and max from the given source Memory.
   * The resulting Combined Buffer is always in non-compact form and must be pre-allocated.
   * @param srcMem the given source Memory
   * @param serVer the serialization version of the source
   * @param srcIsCompact true if the given source Memory is in compact form
   * @param combBufCap total items for the combined buffer (size in doubles)
   */
  private void srcMemoryToCombinedBuffer(final Memory srcMem, final int serVer,
      final boolean srcIsCompact, final int combBufCap) {
    final Object memArr = srcMem.array(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    final int preLongs = 2;
    final int extra = (serVer == 1) ? 3 : 2; // space for min and max values, buf alloc (SerVer 1)
    final int preBytes = (preLongs + extra) << 3;
    final int bbCnt = baseBufferCount_;
    final int k = getK();
    final long n = getN();
    final double[] combinedBuffer = new double[combBufCap]; //always non-compact
    //Load min, max
    putMinValue(extractMinDouble(memArr, memAdd));
    putMaxValue(extractMaxDouble(memArr, memAdd));

    if (srcIsCompact) {
      //Load base buffer
      srcMem.getDoubleArray(preBytes, combinedBuffer, 0, bbCnt);

      //Load levels from compact srcMem
      long bitPattern = bitPattern_;
      if (bitPattern != 0) {
        long memOffset = preBytes + (bbCnt << 3);
        int combBufOffset = 2 * k;
        while (bitPattern != 0L) {
          if ((bitPattern & 1L) > 0L) {
            srcMem.getDoubleArray(memOffset, combinedBuffer, combBufOffset, k);
            memOffset += (k << 3); //bytes, increment compactly
          }
          combBufOffset += k; //doubles, increment every level
          bitPattern >>>= 1;
        }

      }
    } else { //srcMem not compact
      final int levels = Util.computeNumLevelsNeeded(k, n);
      final int totItems = (levels == 0) ? bbCnt : (2 + levels) * k;
      srcMem.getDoubleArray(preBytes, combinedBuffer, 0, totItems);
    }
    putCombinedBuffer(combinedBuffer);
  }

  //Restricted overrides
  //Gets

  @Override
  int getBaseBufferCount() {
    return baseBufferCount_;
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return combinedBufferItemCapacity_;
  }

  @Override
  double[] getCombinedBuffer() {
    return combinedBuffer_;
  }

  @Override
  long getBitPattern() {
    return bitPattern_;
  }

  @Override
  Memory getMemory() {
    return null;
  }

  //Puts

  @Override
  void putMinValue(final double minValue) {
    minValue_ = minValue;
  }

  @Override
  void putMaxValue(final double maxValue) {
    maxValue_ = maxValue;
  }

  @Override
  void putN(final long n) {
    n_ = n;
  }

  @Override
  void putCombinedBuffer(final double[] combinedBuffer) {
    combinedBuffer_ = combinedBuffer;
  }

  @Override
  void putCombinedBufferItemCapacity(final int combinedBufferItemCapacity) {
    combinedBufferItemCapacity_ = combinedBufferItemCapacity;
  }

  @Override
  void putBaseBufferCount(final int baseBufferCount) {
    baseBufferCount_ = baseBufferCount;
  }

  @Override
  void putBitPattern(final long bitPattern) {
    bitPattern_ = bitPattern;
  }

  @Override
  double[] growCombinedBuffer(final int currentSpace, final int spaceNeeded) {
    combinedBuffer_ = Arrays.copyOf(combinedBuffer_, spaceNeeded);
    combinedBufferItemCapacity_ = spaceNeeded;
    return combinedBuffer_;
  }

  /**
   * This is only used for on-heap sketches, and grows the Base Buffer by factors of 2 until it
   * reaches the maximum size of 2 * k. It is only called when there are no levels above the
   * Base Buffer.
   *
   * @param sketch the given sketch.
   */
  //important: n has not been incremented yet
  private final void growBaseBuffer() {
    final double[] baseBuffer = combinedBuffer_;
    final int oldSize = combinedBufferItemCapacity_;
    assert oldSize < 2 * k_;
    final int newSize = 2 * Math.max(Math.min(k_, oldSize), DoublesSketch.MIN_K);
    combinedBufferItemCapacity_ = newSize;
    combinedBuffer_ = Arrays.copyOf(baseBuffer, newSize);
  }

  static void checkPreLongsEmpty(final int preLongs, final boolean empty, final int serVer) {
    final int minPre = Family.QUANTILES.getMinPreLongs(); //1

    final int maxPre = (serVer == 1) ? 5 : Family.QUANTILES.getMaxPreLongs(); //2
    final boolean valid = ((preLongs == minPre) && empty) || ((preLongs == maxPre) && !empty);
    if (!valid) {
      throw new SketchesArgumentException(
          "Possible corruption: PreambleLongs inconsistent with empty state: " + preLongs);
    }
  }

  /**
   * Checks the validity of the heap memory capacity assuming n, k and the compact state.
   * @param k the given value of k
   * @param n the given value of n
   * @param compact true if memory is in compact form
   * @param memCapBytes the current memory capacity in bytes
   */
  static void checkHeapMemCapacity(final int k, final long n, final boolean compact,
      final int serVer, final long memCapBytes) {
    final int metaPre = Family.QUANTILES.getMaxPreLongs() + ((serVer == 1) ? 3 : 2);
    final int retainedItems = computeRetainedItems(k, n);
    final int reqBufBytes;
    if (compact) {
      reqBufBytes = (metaPre + retainedItems) << 3;
    } else { //not compact
      final int totLevels = Util.computeNumLevelsNeeded(k, n);
      reqBufBytes = (totLevels == 0)
          ? (metaPre + retainedItems) << 3
          : (metaPre + (2 + totLevels) * k) << 3;
    }
    if (memCapBytes < reqBufBytes) {
      throw new SketchesArgumentException("Possible corruption: Memory capacity too small: "
          + memCapBytes + " < " + reqBufBytes);
    }
  }

}
