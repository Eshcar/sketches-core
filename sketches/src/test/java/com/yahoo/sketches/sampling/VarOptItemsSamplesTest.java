/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ConcurrentModificationException;

import org.testng.annotations.Test;

/**
 * @author Jon Malkin
 */
public class VarOptItemsSamplesTest {
  @Test
  public void compareIteratorToArrays() {
    final int k = 64;
    final int n = 1024;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketchTest.getUnweightedLongsVIS(k, n);
    // and a few heavy items
    sketch.update(n * 10L, n * 10.0);
    sketch.update(n * 11L, n * 11.0);
    sketch.update(n * 12L, n * 12.0);

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    final Long[] items = samples.items();
    final double[] weights = samples.weights();
    int i = 0;
    for (VarOptItemsSamples<Long>.WeightedSample ws : samples) {
      assertEquals(ws.getWeight(), weights[i]);
      assertEquals(ws.getItem(), items[i]);
      ++i;
    }
  }

  @Test(expectedExceptions = ConcurrentModificationException.class)
  public void checkConcurrentModification() {
    final int k = 128;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketchTest.getUnweightedLongsVIS(k, k);

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();

    for (VarOptItemsSamples<Long>.WeightedSample ws : samples) {
      if (ws.getItem() > (k / 2)) { // guaranteed to exist somewhere in the sketch
        sketch.update(-1L, 1.0);
      }
    }
    fail();
  }

  @Test
  public void checkPolymorphicBaseClass() {
    // use setClass()
    final VarOptItemsSketch<Number> sketch = VarOptItemsSketch.getInstance(12);
    sketch.update(1, 0.5);
    sketch.update(2L, 1.7);
    sketch.update(3.0f, 2.0);
    sketch.update(4.0, 3.14159);

    try {
      final VarOptItemsSamples samples = sketch.getSketchSamples();
      // will try to create array based on type of first item in the array and fail
      samples.items();
      fail();
    } catch (final ArrayStoreException e) {
      // expected
    }

    final VarOptItemsSamples<Number> samples = sketch.getSketchSamples();
    samples.setClass(Number.class);
    assertEquals(samples.items(0), 1);
    assertEquals(samples.items(1), 2L);
    assertEquals(samples.items(2), 3.0f);
    assertEquals(samples.items(3), 4.0);
  }

  @Test
  public void checkEmptySketch() {
    // verify what happens when n_ == 0
    final VarOptItemsSketch<String> sketch = VarOptItemsSketch.getInstance(32);
    assertEquals(sketch.getNumSamples(), 0);

    final VarOptItemsSamples<String> samples = sketch.getSketchSamples();
    assertEquals(samples.getNumSamples(), 0);
    assertNull(samples.items());
    assertNull(samples.items(0));
    assertNull(samples.weights());
    assertEquals(samples.weights(0), Double.NaN);
  }

  @Test
  public void checkUnderFullSketchIterator() {
    // needed to fully cover iterator's next() and hasNext() conditions
    final int k = 128;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketchTest.getUnweightedLongsVIS(k, k / 2);

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();

    for (VarOptItemsSamples<Long>.WeightedSample ws : samples) {
      assertTrue(ws.getItem() >= 0 && ws.getItem() < (k / 2));
    }
  }
}
