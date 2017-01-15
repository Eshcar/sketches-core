/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.HeapDoublesSketchTest.buildQS;
import static com.yahoo.sketches.quantiles.Util.LS;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

public class DoublesUtilTest {

  @Test
  public void checkPrintMemData() {
    int k = 16;//DoublesSketch.DEFAULT_K;
    int n = 1000;
    DoublesSketch qs = buildQS(k,n);

    byte[] byteArr = qs.toByteArray(true, false);
    Memory mem = new NativeMemory(byteArr);
    println(DoublesUtil.memToString(true, true, mem));

    byteArr = qs.toByteArray(true, true);
    mem = new NativeMemory(byteArr);
    println(DoublesUtil.memToString(true, true, mem));
  }

  @Test
  public void checkPrintMemData2() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 0;
    DoublesSketch qs = buildQS(k,n);

    byte[] byteArr = qs.toByteArray();
    Memory mem = new NativeMemory(byteArr);
    println(DoublesUtil.memToString(true, true, mem));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s+LS);
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    //System.out.print(s); //disable here
  }

}
