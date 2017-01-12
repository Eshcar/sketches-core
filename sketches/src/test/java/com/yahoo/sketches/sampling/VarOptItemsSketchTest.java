/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

public class VarOptItemsSketchTest {
  private static final double EPS = 1e-10;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidK() {
    VarOptItemsSketch.<Integer>getInstance(0);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(16, 16);
    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory mem = new NativeMemory(bytes);

    mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(32, 16);
    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory mem = new NativeMemory(bytes);

    mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

    VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    fail();
  }

  @Test
  public void checkBadPreLongs() {
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(32, 33);
    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory mem = new NativeMemory(bytes);

    // corrupt the preLongs count to 0
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) (Family.VAROPT.getMinPreLongs() - 1));
    try {
      VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 2); // corrupt the preLongs count to 2
    try {
      VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // corrupt the preLongs count to be too large
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) (Family.VAROPT.getMaxPreLongs() + 1));
    try {
      VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadMemory() {
    byte[] bytes = new byte[4];
    Memory mem = new NativeMemory(bytes);

    try {
      PreambleUtil.getAndCheckPreLongs(mem);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    bytes = new byte[8];
    bytes[0] = 2; // only 1 preLong worth of items in bytearray
    mem = new NativeMemory(bytes);
    PreambleUtil.getAndCheckPreLongs(mem);
  }

  @Test
  public void checkMalformedPreamble() {
    final int k = 50;
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(k, k);

    final byte[] sketchBytes = sketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory srcMem = new NativeMemory(sketchBytes);

    // we'll use the same initial sketch a few times, so grab a copy of it
    final byte[] copyBytes = new byte[sketchBytes.length];
    final Memory mem = new NativeMemory(copyBytes);
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);

    // copy the bytes
    NativeMemory.copy(srcMem, 0, mem, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractPreLongs(memObj, memAddr), PreambleUtil.VO_WARMUP_PRELONGS);

    // no items in R but max preLongs
    try {
      PreambleUtil.insertPreLongs(memObj, memAddr, Family.VAROPT.getMaxPreLongs());
      VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().startsWith("Possible Corruption: "
              + Family.VAROPT.getMaxPreLongs() + " preLongs but"));
    }

    // refresh the copy
    NativeMemory.copy(srcMem, 0, mem, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractPreLongs(memObj, memAddr), PreambleUtil.VO_WARMUP_PRELONGS);

    // negative H region count
    try {
      PreambleUtil.insertHRegionItemCount(memObj, memAddr, -1);
      VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: H region count cannot be negative: -1"));
    }

    // refresh the copy
    NativeMemory.copy(srcMem, 0, mem, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractHRegionItemCount(memObj, memAddr), k);

    // negative R region count
    try {
      PreambleUtil.insertRRegionItemCount(memObj, memAddr, -128);
      VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: R region count cannot be negative: -128"));
    }

    // refresh the copy
    NativeMemory.copy(srcMem, 0, mem, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractRRegionItemCount(memObj, memAddr), 0);

    // invalid k < 2
    try {
      PreambleUtil.insertK(memObj, memAddr, 0);
      VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: k must be at least 2: 0"));
    }

    // refresh the copy
    NativeMemory.copy(srcMem, 0, mem, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractK(memObj, memAddr), k);

    // invalid n < 0
    try {
      PreambleUtil.insertN(memObj, memAddr, -1024);
      VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: n cannot be negative: -1024"));
    }
  }

  @Test
  public void checkEmptySketch() {
    final VarOptItemsSketch<String> vis = VarOptItemsSketch.getInstance(5);
    assertEquals(vis.getN(), 0);
    assertEquals(vis.getNumSamples(), 0);
    assertNull(vis.getSamplesAsArrays());
    assertNull(vis.getSamplesAsArrays(Long.class));

    final byte[] sketchBytes = vis.toByteArray(new ArrayOfStringsSerDe());
    final Memory mem = new NativeMemory(sketchBytes);

    // only minPreLongs bytes and should deserialize to empty
    assertEquals(sketchBytes.length, Family.VAROPT.getMinPreLongs() << 3);
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    final VarOptItemsSketch<String> loadedVis = VarOptItemsSketch.getInstance(mem, serDe);
    assertEquals(loadedVis.getNumSamples(), 0);

    println("Empty sketch:");
    println("  Preamble:");
    println(PreambleUtil.preambleToString(mem));
    println("  Sketch:");
    println(vis.toString());
  }

  @Test
  public void checkNonEmptyDegenerateSketch() {
    // make an empty serialized sketch, then copy the items into a
    // PreambleUtil.VO_WARMUP_PRELONGS-sized byte array
    // so there'll be no items, then clear the empty flag so it will try to load
    // the rest.
    final VarOptItemsSketch<String> vis = VarOptItemsSketch.getInstance(12, ResizeFactor.X2);
    final byte[] sketchBytes = vis.toByteArray(new ArrayOfStringsSerDe());
    final byte[] dstByteArr = new byte[PreambleUtil.VO_WARMUP_PRELONGS << 3];
    final Memory mem = new NativeMemory(dstByteArr);
    mem.putByteArray(0, sketchBytes, 0, sketchBytes.length);

    // ensure non-empty but with H and R region sizes set to 0
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);
    PreambleUtil.insertFlags(memObj, memAddr, 0); // set not-empty
    PreambleUtil.insertHRegionItemCount(memObj, memAddr, 0);
    PreambleUtil.insertRRegionItemCount(memObj, memAddr, 0);

    final VarOptItemsSketch<String> rebuilt
            = VarOptItemsSketch.getInstance(mem, new ArrayOfStringsSerDe());
    assertNotNull(rebuilt);
    assertEquals(rebuilt.getNumSamples(), 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidWeight() {
    final VarOptItemsSketch<String> vis = VarOptItemsSketch.getInstance(5);
    try {
      vis.update(null, 1.0); // should work fine
    } catch (final SketchesArgumentException e) {
      fail();
    }

    vis.update("invalidWeight", -1.0); // should fail
  }

  @Test
  public void checkCorruptSerializedWeight() {
    final VarOptItemsSketch<String> vis = VarOptItemsSketch.getInstance(24);
    for (int i = 1; i < 10; ++i) {
      vis.update(Integer.toString(i), (double) i);
    }

    final byte[] sketchBytes = vis.toByteArray(new ArrayOfStringsSerDe(), String.class);
    final Memory mem = new NativeMemory(sketchBytes);
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);

    // weights will be stored in the first double after the preamble
    final int numPreLongs = PreambleUtil.extractPreLongs(memObj, memAddr);
    final int weightOffset = numPreLongs << 3;
    mem.putDouble(weightOffset, -1.25); // inject a negative weight

    try {
      VarOptItemsSketch.getInstance(mem, new ArrayOfStringsSerDe());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: Non-positive weight in "
              + "getInstance(): -1.25"));
    }
  }

  @Test
  public void checkCumulativeWeight() {
    final int k = 256;
    final int n = 10 * k;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.getInstance(k);

    double inputSum = 0.0;
    for (long i = 0; i < n; ++i) {
      // generate weights above and below 1.0 using w ~ exp(5*N(0,1)) which covers about
      // 10 orders of magnitude
      final double w = Math.exp(5 * SamplingUtil.rand.nextGaussian());
      inputSum += w;
      sketch.update(i, w);
    }

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    double outputSum = 0;
    for (VarOptItemsSamples<Long>.WeightedSample ws : samples) {
      outputSum += ws.getWeight();
    }

    final double wtRatio = outputSum / inputSum;
    assertTrue(Math.abs(wtRatio - 1.0) < EPS);
  }

  @Test
  public void checkUnderFullSketchSerialization() {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.getInstance(2048);
    for (long i = 0; i < 10; ++i) {
      sketch.update(i, 1.0);
    }
    assertEquals(sketch.getNumSamples(), 10);

    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory mem = new NativeMemory(bytes);

    // ensure 2 preLongs
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);
    assertEquals(PreambleUtil.extractPreLongs(memObj, memAddr), PreambleUtil.VO_WARMUP_PRELONGS);

    final VarOptItemsSketch<Long> rebuilt
            = VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    checkIfEqual(rebuilt, sketch);
  }

  @Test
  public void checkEndOfWarmupSketchSerialization() {
    final int k = 2048;
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(k, k);

    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory mem = new NativeMemory(bytes);

    // ensure still only 2 preLongs
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);
    assertEquals(PreambleUtil.extractPreLongs(memObj, memAddr), PreambleUtil.VO_WARMUP_PRELONGS);

    final VarOptItemsSketch<Long> rebuilt
            = VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    checkIfEqual(rebuilt, sketch);
  }

  @Test
  public void checkFullSketchSerialization() {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.getInstance(32);
    for (long i = 0; i < 32; ++i) {
      sketch.update(i, 1.0);
    }
    sketch.update(100L, 100.0);
    sketch.update(101L, 101.0);
    assertEquals(sketch.getNumSamples(), 32);

    // first 2 entries should be heavy and in heap order (smallest at root)
    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    final Long[] data = samples.items();
    final double[] weights = samples.weights();
    assertEquals(weights[0], 100.0);
    assertEquals(weights[1], 101.0);
    assertEquals((long) data[0], 100L);
    assertEquals((long) data[1], 101L);

    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory mem = new NativeMemory(bytes);

    // ensure 3 preLongs
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);
    assertEquals(PreambleUtil.extractPreLongs(memObj, memAddr), Family.VAROPT.getMaxPreLongs());

    final VarOptItemsSketch<Long> rebuilt
            = VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    checkIfEqual(rebuilt, sketch);
  }

  @Test
  public void checkPseudoLightUpdate() {
    final int k = 1024;
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(k, k + 1);
    sketch.update(0L, 1.0); // k+2-nd update

    // checking weights(0), assuming all k items are unweighted (and consequently in R)
    // Expected: (k + 2) / |R| = (k+2) / k
    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    final double wtDiff = samples.weights(0) - 1.0 * (k + 2) / k;
    assertTrue(Math.abs(wtDiff) < EPS);
  }

  @Test
  public void checkPseudoHeavyUpdates() {
    final int k = 1024;
    final double wtScale = 10.0 * k;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.getInstance(k);
    for (long i = 0; i <= k; ++i) {
      sketch.update(i, 1.0);
    }

    // Next k-1 updates should be updatePseudoHeavyGeneral()
    // Last one should call updatePseudoHeavyREq1(), since we'll have added k-1 heavy
    // items, leaving only 1 item left in R
    for (long i = 1; i <= k; ++i) {
      sketch.update(-i, k + (i * wtScale));
    }

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    final double[] weights = samples.weights();

    // Don't know which R item is left, but should be only one at the end of the array
    // Expected: k+1 + (min "heavy" item) / |R| = ((k+1) + (k+wtScale)) / 1 = wtScale + 2k + 1
    double wtDiff = weights[k - 1] - 1.0 * (wtScale + 2 * k + 1);
    assertTrue(Math.abs(wtDiff) < EPS);

    // Expected: 2nd lightest "heavy" item: k + 2*wtScale
    wtDiff = weights[0] - 1.0 * (k + 2 * wtScale);
    assertTrue(Math.abs(wtDiff) < EPS);
  }


  /* Returns a sketch of size k that has been presented with n items. Use n = k+1 to obtain a
     sketch that has just reached the sampling phase, so that the next update() is handled by
     one of the non-warmup routes.
   */
  static VarOptItemsSketch<Long> getUnweightedLongsVIS(final int k, final int n) {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.getInstance(k);
    for (long i = 0; i < n; ++i) {
      sketch.update(i, 1.0);
    }

    return sketch;
  }

  private <T> void checkIfEqual(final VarOptItemsSketch<T> s1, final VarOptItemsSketch<T> s2) {
    assertEquals(s1.getK(), s2.getK(), "Sketches have different values of k");
    assertEquals(s1.getNumSamples(), s2.getNumSamples(), "Sketches have different sample counts");

    final int len = s1.getNumSamples();
    final VarOptItemsSamples<T> r1 = s1.getSketchSamples();
    final VarOptItemsSamples<T> r2 = s2.getSketchSamples();

    // next 2 lines also trigger copying results
    assertEquals(len, r1.getNumSamples());
    assertEquals(r1.getNumSamples(), r2.getNumSamples());

    for (int i = 0; i < len; ++i) {
      assertEquals(r1.items(i), r2.items(i), "Data values differ at sample " + i);
      assertEquals(r1.weights(i), r2.weights(i), "Weights differ at sample " + i);
    }
  }

  /**
   * Wrapper around System.out.println() allowing a simple way to disable logging in tests
   * @param msg The message to print
   */
  private static void println(final String msg) {
    //System.out.println(msg);
  }
}
