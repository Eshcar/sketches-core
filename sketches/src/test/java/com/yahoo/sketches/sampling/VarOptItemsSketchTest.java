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
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.hll.Preamble;

public class VarOptItemsSketchTest {
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidK() {
    VarOptItemsSketch.<Integer>getInstance(0);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final Memory mem = getSmallSerializedLongsVIS();
    mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    final Memory mem = getSmallSerializedLongsVIS();
    mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

    VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreLongs() {
    final Memory mem = getSmallSerializedLongsVIS();
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

    VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    fail();
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
    bytes[0] = 2; // only 1 preLong worth of data in bytearray
    mem = new NativeMemory(bytes);
    PreambleUtil.getAndCheckPreLongs(mem);
  }

  @Test
  public void checkEmptySketch() {
    final VarOptItemsSketch<String> vis = VarOptItemsSketch.getInstance(5);
    assertTrue(vis.getSamples() == null);

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
  public void checkUnderfullSketchSerialization() {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.getInstance(20);
    for (long i = 0; i < 10; ++i) {
      sketch.update(i, 1.0);
    }
    assertEquals(sketch.getNumSamples(), 10);

    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory mem = new NativeMemory(bytes);

    // ensure 2 preLongs
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);
    assertEquals(PreambleUtil.extractPreLongs(memObj, memAddr), 2);

    final VarOptItemsSketch<Long> rebuilt
            = VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    assertEquals(rebuilt.getNumSamples(), 10);

    final VarOptItemsSketch.Result samples = rebuilt.getSamples();
    for (int i = 0; i < rebuilt.getNumSamples(); ++i) {
      assertEquals(samples.data[i], (long) i);
      assertEquals(samples.weights[i], 1.0);
    }
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

    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory mem = new NativeMemory(bytes);

    // ensure 3 preLongs
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);
    assertEquals(PreambleUtil.extractPreLongs(memObj, memAddr), 3);

    final VarOptItemsSketch<Long> rebuilt
            = VarOptItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    assertEquals(rebuilt.getNumSamples(), 32);

    // check consistency
    final VarOptItemsSketch.Result orig = sketch.getSamples();
    final VarOptItemsSketch.Result tgt = rebuilt.getSamples();
    for (int i = 0; i < rebuilt.getNumSamples(); ++i) {
      assertEquals(tgt.data[i], orig.data[i]);
      assertEquals(tgt.weights[i], orig.weights[i]);
    }

    // first 2 entries should be heavy and in heap order (smallest at root)
    assertTrue(orig.weights[0] == 100.0);
    assertTrue(orig.weights[1] == 101.0);
  }



  private Memory getSmallSerializedLongsVIS() {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.getInstance(5);
    for (long i = 1; i <= 5; ++i) {
      sketch.update(i, 1.0);
      //System.out.println(sketch);
    }
    sketch.update(10L, Math.sqrt(70.0));
    //sketch.update(7L, Math.sqrt(1.0));

    System.out.println(sketch);
    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());

    return new NativeMemory(bytes);
  }

  private Memory getLargeSerializedLongsVIS() {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.getInstance(1024);
    for (long i = 1; i <= 1024; ++i) {
      sketch.update(i, 1.0);
      //System.out.println(sketch);
    }
    sketch.update(10L, Math.sqrt(70.0));
    //sketch.update(7L, Math.sqrt(1.0));

    System.out.println(sketch);
    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe());

    return new NativeMemory(bytes);
  }

  /**
   * Wrapper around System.out.println() allowing a simple way to disable logging in tests
   * @param msg The message to print
   */
  private static void println(String msg) {
    //System.out.println(msg);
  }
}
