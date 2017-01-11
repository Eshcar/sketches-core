/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class UnionImplTest {

  @Test
  public void checkUpdateWithSketch() {
    int k = 16;
    Memory mem = new NativeMemory(new byte[k*8 + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().build(k);
    for (int i=0; i<k; i++) sketch.update(i); //exact
    CompactSketch sketchInDirectOrd = sketch.compact(true, mem);
    CompactSketch sketchInDirectUnord = sketch.compact(false, mem);
    CompactSketch sketchInHeap = sketch.compact(true, null);

    Union union = Sketches.setOperationBuilder().buildUnion(k);
    union.update(sketchInDirectOrd);
    union.update(sketchInHeap);
    union.update(sketchInDirectUnord);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
  }

  @Test
  public void checkUpdateWithMem() {
    int k = 16;
    Memory skMem = new NativeMemory(new byte[2*k*8 + 24]);
    Memory doCskMem = new NativeMemory(new byte[k*8 + 24]);
    Memory duCskMem = new NativeMemory(new byte[k*8 + 24]);
    UpdateSketch udSketch = UpdateSketch.builder().initMemory(skMem).build(k);
    for (int i = 0; i < k; i++) { udSketch.update(i); } //exact
    udSketch.compact(true, doCskMem);
    udSketch.compact(false, duCskMem);

    Union union = Sketches.setOperationBuilder().buildUnion(k);
    union.update(skMem);
    union.update(doCskMem);
    union.update(duCskMem);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
  }


  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptFamilyException() {
    int k = 16;
    Memory mem = new NativeMemory(new byte[k*8 + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().build(k);
    for (int i=0; i<k; i++) sketch.update(i);
    sketch.compact(true, mem);

    mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    Union union = Sketches.setOperationBuilder().buildUnion(k);
    union.update(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkVer1FamilyException() {
    int k = 16;
    Memory v3mem = new NativeMemory(new byte[k*8 + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().build(k);
    for (int i=0; i<k; i++) sketch.update(i);
    sketch.compact(true, v3mem);
    Memory v1mem = ForwardCompatibilityTest.convertSerV3toSerV1(v3mem);

    v1mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)2); //corrupt family

    Union union = Sketches.setOperationBuilder().buildUnion(k);
    union.update(v1mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkVer2FamilyException() {
    int k = 16;
    Memory v3mem = new NativeMemory(new byte[k*8 + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().build(k);
    for (int i=0; i<k; i++) sketch.update(i);
    sketch.compact(true, v3mem);
    Memory v2mem = ForwardCompatibilityTest.convertSerV3toSerV2(v3mem);

    v2mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)2); //corrupt family

    Union union = Sketches.setOperationBuilder().buildUnion(k);
    union.update(v2mem);
  }


  public static void main(String[] args) {
    UnionImplTest test = new UnionImplTest();
    test.checkUpdateWithSketch();
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
