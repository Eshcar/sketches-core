package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_INT;
import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_SHORT;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfNumbersSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesException;
import com.yahoo.sketches.SketchesStateException;

public class ReservoirItemsSketchTest {
  private static final double EPS = 1e-8;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidK() {
    ReservoirItemsSketch.<Integer>getInstance(0);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    Memory mem = getBasicSerializedLongsRIS();
    mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    ReservoirItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    Memory mem = getBasicSerializedLongsRIS();
    mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

    ReservoirItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreLongs() {
    Memory mem = getBasicSerializedLongsRIS();
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

    ReservoirItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadMemory() {
    byte[] bytes = new byte[4];
    Memory mem = new NativeMemory(bytes);

    try {
      PreambleUtil.getAndCheckPreLongs(mem);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }

    bytes = new byte[8];
    bytes[0] = 2; // only 1 preLong worth of data in bytearray
    mem = new NativeMemory(bytes);
    PreambleUtil.getAndCheckPreLongs(mem);
  }


  @Test
  public void checkEmptySketch() {
    ReservoirItemsSketch<String> ris = ReservoirItemsSketch.getInstance(5);
    assertTrue(ris.getSamples() == null);

    byte[] sketchBytes = ris.toByteArray(new ArrayOfStringsSerDe());
    Memory mem = new NativeMemory(sketchBytes);

    // only minPreLongs bytes and should deserialize to empty
    assertEquals(sketchBytes.length, Family.RESERVOIR.getMinPreLongs() << 3);
    ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    ReservoirItemsSketch<String> loadedRis = ReservoirItemsSketch.getInstance(mem, serDe);
    assertEquals(loadedRis.getNumSamples(), 0);

    println("Empty sketch:");
    println("  Preamble:");
    println(PreambleUtil.preambleToString(mem));
    println("  Sketch:");
    println(ris.toString());
  }

  @Test
  public void checkUnderFullReservoir() {
    int k = 128;
    int n = 64;

    ReservoirItemsSketch<String> ris = ReservoirItemsSketch.getInstance(k);
    int expectedLength = 0;

    for (int i = 0; i < n; ++i) {
      String intStr = Integer.toString(i);
      expectedLength += intStr.length() + Integer.BYTES;
      ris.update(intStr);
    }
    assertEquals(ris.getNumSamples(), n);

    String[] data = ris.getSamples();
    assertEquals(ris.getNumSamples(), ris.getN());
    assertEquals(data.length, n);

    // items in submit order until reservoir at capacity so check
    for (int i = 0; i < n; ++i) {
      assertEquals(data[i], Integer.toString(i));
    }

    // not using validateSerializeAndDeserialize() to check with a non-Long
    expectedLength += Family.RESERVOIR.getMaxPreLongs() << 3;
    byte[] sketchBytes = ris.toByteArray(new ArrayOfStringsSerDe());
    assertEquals(sketchBytes.length, expectedLength);

    // ensure reservoir rebuilds correctly
    Memory mem = new NativeMemory(sketchBytes);
    ReservoirItemsSketch<String> loadedRis = ReservoirItemsSketch.getInstance(mem,
            new ArrayOfStringsSerDe());

    validateReservoirEquality(ris, loadedRis);

    println("Under-full reservoir:");
    println("  Preamble:");
    println(PreambleUtil.preambleToString(mem));
    println("  Sketch:");
    println(ris.toString());
  }

  @Test
  public void checkFullReservoir() {
    int k = 1000;
    int n = 2000;

    // specify smaller ResizeFactor to ensure multiple resizes
    ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.getInstance(k, ResizeFactor.X2);

    for (int i = 0; i < n; ++i) {
      ris.update((long) i);
    }
    assertEquals(ris.getNumSamples(), ris.getK());

    validateSerializeAndDeserialize(ris);

    println("Full reservoir:");
    println("  Preamble:");
    println(PreambleUtil.preambleToString(ris.toByteArray(new ArrayOfLongsSerDe())));
    println("  Sketch:");
    println(ris.toString());
  }

  @Test
  public void checkPolymorphicType() {
    ReservoirItemsSketch<Number> ris = ReservoirItemsSketch.getInstance(6);

    assertNull(ris.getSamples());
    assertNull(ris.getSamples(Number.class));

    // using mixed types
    ris.update(1);
    ris.update(2L);
    ris.update(3.0);
    ris.update((short) (44023 & 0xFFFF));
    ris.update((byte) (68 & 0xFF));
    ris.update(4.0F);

    Number[] data = ris.getSamples(Number.class);
    assertNotNull(data);
    assertEquals(data.length, 6);

    // copying samples without specifying Number.class should fail
    try {
      ris.getSamples();
      fail();
    } catch (ArrayStoreException e) {
      // expected
    }

    // likewise for toByteArray() (which uses getDataSamples() internally for type handling)
    ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();
    try {
      ris.toByteArray(serDe);
      fail();
    } catch (ArrayStoreException e) {
      // expected
    }

    byte[] sketchBytes = ris.toByteArray(serDe, Number.class);
    assertEquals(sketchBytes.length, 49);

    Memory mem = new NativeMemory(sketchBytes);
    ReservoirItemsSketch<Number> loadedRis = ReservoirItemsSketch.getInstance(mem, serDe);

    assertEquals(ris.getNumSamples(), loadedRis.getNumSamples());

    Number[] samples1 = ris.getSamples(Number.class);
    Number[] samples2 = loadedRis.getSamples(Number.class);
    assertEquals(samples1.length, samples2.length);

    for (int i = 0; i < samples1.length; ++i) {
      assertEquals(samples1[i], samples2[i]);
    }
  }

  @Test
  public void checkBadConstructorArgs() {
    ArrayList<String> data = new ArrayList<>(128);
    for (int i = 0; i < 128; ++i) {
      data.add(Integer.toString(i));
    }

    ResizeFactor rf = ResizeFactor.X8;

    // no data
    try {
      ReservoirItemsSketch.<Byte>getInstance(null, 128, rf, 128);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("null reservoir"));
    }

    // size too small
    try {
      ReservoirItemsSketch.getInstance(data, 128, rf, 1);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("size less than 2"));
    }

    // configured reservoir size smaller than data length
    try {
      ReservoirItemsSketch.getInstance(data, 128, rf, 64);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("max size less than array length"));
    }

    // too many items seen vs data length, full sketch
    try {
      ReservoirItemsSketch.getInstance(data, 512, rf, 256);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("too few samples"));
    }

    // too many items seen vs data length, under-full sketch
    try {
      ReservoirItemsSketch.getInstance(data, 256, rf, 256);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("too few samples"));
    }
  }

  @Test
  public void checkRawSamples() {
    int  k = 32;
    long n = 12;
    ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.getInstance(k, ResizeFactor.X2);

    for (long i = 0; i < n; ++i) {
      ris.update(i);
    }

    Long[] samples = ris.getSamples();
    assertEquals(samples.length, n);

    ArrayList<Long> rawSamples = ris.getRawSamplesAsList();
    assertEquals(rawSamples.size(), n);

    // change a value and make sure getDataSamples() reflects that change
    assertEquals((long) rawSamples.get(0), 0L);
    rawSamples.set(0, -1L);

    samples = ris.getSamples();
    assertEquals((long) samples[0], -1L);
    assertEquals(samples.length, n);
  }


  @Test
  public void checkSketchCapacity() {
    ArrayList<Long> data = new ArrayList<>(64);
    for (long i = 0; i < 64; ++i) {
      data.add(i);
    }
    long itemsSeen = (1L << 48) - 2;

    ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.getInstance(data, itemsSeen,
            ResizeFactor.X8, 64);

    // this should work, the next should fail
    ris.update(0L);

    try {
      ris.update(0L);
      fail();
    } catch (SketchesStateException e) {
      assertTrue(e.getMessage().contains("Sketch has exceeded capacity for total items seen"));
    }
  }

  @Test
  public void checkSampleWeight() {
    int k = 32;
    ReservoirItemsSketch<Integer> ris = ReservoirItemsSketch.getInstance(k);

    for (int i = 0; i < (k / 2); ++i) {
      ris.update(i);
    }
    assertEquals(ris.getImplicitSampleWeight(), 1.0); // should be exact value here

    // will have 3k/2 total samples when done
    for (int i = 0; i < k; ++i) {
      ris.update(i);
    }
    assertTrue(ris.getImplicitSampleWeight() - 1.5 < EPS);
  }

  @Test
  public void checkVersionConversion() {
    // version change from 1 to 2 only impact first preamble long, so empty sketch is sufficient
    int k = 32768;
    short encK = ReservoirSize.computeSize(k);
    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();

    ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.getInstance(k);
    byte[] sketchBytesOrig = ris.toByteArray(serDe);

    // get a new byte[], manually revert to v1, then reconstruct
    byte[] sketchBytes = ris.toByteArray(serDe);
    Memory sketchMem = new NativeMemory(sketchBytes);

    sketchMem.putByte(SER_VER_BYTE, (byte) 1);
    sketchMem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    sketchMem.putShort(RESERVOIR_SIZE_SHORT, encK);

    ReservoirItemsSketch<Long> rebuilt = ReservoirItemsSketch.getInstance(sketchMem, serDe);
    byte[] rebuiltBytes = rebuilt.toByteArray(serDe);

    assertEquals(sketchBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < sketchBytesOrig.length; ++i) {
      assertEquals(sketchBytesOrig[i], rebuiltBytes[i]);
    }
  }

  @Test
  public void checkSetAndGetValue() {
    int k = 20;
    short tgtIdx = 5;
    ReservoirItemsSketch<Short> ris = ReservoirItemsSketch.getInstance(k);

    ris.update(null);
    assertEquals(ris.getN(), 0);

    for (short i = 0; i < k; ++i) {
      ris.update(i);
    }

    assertEquals((short) ris.getValueAtPosition(tgtIdx), tgtIdx);
    ris.insertValueAtPosition((short) -1, tgtIdx);
    assertEquals((short) ris.getValueAtPosition(tgtIdx), (short) -1);
  }

  @Test
  public void checkBadSetAndGetValue() {
    int k = 20;
    int tgtIdx = 5;
    ReservoirItemsSketch<Integer> ris = ReservoirItemsSketch.getInstance(k);

    try {
      ris.getValueAtPosition(0);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }

    for (int i = 0; i < k; ++i) {
      ris.update(i);
    }
    assertEquals((int) ris.getValueAtPosition(tgtIdx), tgtIdx);

    try {
      ris.insertValueAtPosition(-1, -1);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }

    try {
      ris.insertValueAtPosition(-1, k + 1);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }

    try {
      ris.getValueAtPosition(-1);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }

    try {
      ris.getValueAtPosition(k + 1);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkForceIncrement() {
    int k = 100;
    ReservoirItemsSketch<Long> rls = ReservoirItemsSketch.getInstance(k);

    for (long i = 0; i < 2 * k; ++i) {
      rls.update(i);
    }

    assertEquals(rls.getN(), 2 * k);
    rls.forceIncrementItemsSeen(k);
    assertEquals(rls.getN(), 3 * k);

    try {
      rls.forceIncrementItemsSeen((1L << 48) - 2);
      fail();
    } catch (SketchesStateException e) {
      // expected
    }
  }

  static Memory getBasicSerializedLongsRIS() {
    int k = 10;
    int n = 20;

    ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.getInstance(k);
    assertEquals(ris.getNumSamples(), 0);

    for (int i = 0; i < n; ++i) {
      ris.update((long) i);
    }
    assertEquals(ris.getNumSamples(), Math.min(n, k));
    assertEquals(ris.getN(), n);
    assertEquals(ris.getK(), k);

    byte[] sketchBytes = ris.toByteArray(new ArrayOfLongsSerDe());
    return new NativeMemory(sketchBytes);
  }

  static void validateSerializeAndDeserialize(ReservoirItemsSketch<Long> ris) {
    byte[] sketchBytes = ris.toByteArray(new ArrayOfLongsSerDe());
    assertEquals(sketchBytes.length,
            (Family.RESERVOIR.getMaxPreLongs() + ris.getNumSamples()) << 3);

    // ensure full reservoir rebuilds correctly
    Memory mem = new NativeMemory(sketchBytes);
    ReservoirItemsSketch<Long> loadedRis = ReservoirItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());

    validateReservoirEquality(ris, loadedRis);
  }

  static <T> void validateReservoirEquality(ReservoirItemsSketch<T> ris1,
                                            ReservoirItemsSketch<T> ris2) {
    assertEquals(ris1.getNumSamples(), ris2.getNumSamples());

    if (ris1.getNumSamples() == 0) { return; }

    Object[] samples1 = ris1.getSamples();
    Object[] samples2 = ris2.getSamples();
    assertEquals(samples1.length, samples2.length);

    for (int i = 0; i < samples1.length; ++i) {
      assertEquals(samples1[i], samples2[i]);
    }
  }

  /**
   * Wrapper around System.out.println() allowing a simple way to disable logging in tests
   * @param msg The message to print
   */
  private static void println(String msg) {
    //System.out.println(msg);
  }
}
