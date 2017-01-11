package com.yahoo.sketches.sampling;

import java.util.ConcurrentModificationException;
import java.util.Iterator;

/**
 * @author Jon Malkin
 */
public class VarOptItemsSketchSamples<T> implements
        Iterable<VarOptItemsSketchSamples<T>.WeightedSample> {

  private final VarOptItemsSketch<T> sketch_;
  private VarOptItemsSketch.Result sampleLists;
  private final long n_;
  private final double rWeight_;
  private final int h_;

  public class WeightedSample {
    private final int idx_;

    WeightedSample(final int i) {
      idx_ = i;
    }

    public T getItem() {
      return sketch_.getItem(idx_);
    }

    public double getWeight() {
      return idx_ > h_ ? rWeight_ : sketch_.getWeight(idx_);
    }
  }

  public class VarOptItemsIterator implements Iterator<WeightedSample> {
    private int currIdx_ = 0;

    @Override
    public boolean hasNext() {
      // If sketch is in exact mode, we'll have a next item as long as index < k.
      // If in sampling mode, the last index is k (array length k+1) but there will always be at
      // least one item in R, so no need to check if the last element is null.
      final int k = sketch_.getK();
      return (n_ <= k && currIdx_ < n_) || (n_ > k && currIdx_ <= k);
    }

    @Override
    public WeightedSample next() {
      if (n_ != sketch_.getN()) {
        throw new ConcurrentModificationException();
      }

      // grab current index, apply logic to update currIdx_ for the next call
      final int tgt = currIdx_;

      ++currIdx_;
      if (currIdx_ == h_ && h_ != n_) {
        ++currIdx_;
      }

      return new WeightedSample(tgt);
    }
  }

  VarOptItemsSketchSamples(final VarOptItemsSketch<T> sketch) {
    sketch_ = sketch;
    n_ = sketch.getN();
    h_ = sketch.getHRegionCount();
    rWeight_ = sketch.getRRegionWeight();
  }

  @Override
  public Iterator<WeightedSample> iterator() {
    return new VarOptItemsIterator();
  }

  public void setClass(final Class clazz) {
    sampleLists = sketch_.getSamples(clazz);
  }

  @SuppressWarnings("unchecked")
  public T[] items() {
    if (sampleLists == null) {
      sampleLists = sketch_.getSamples();
    }

    return (sampleLists == null ? null : (T[]) sampleLists.data);
  }

  @SuppressWarnings("unchecked")
  public T items(final int i) {
    if (sampleLists == null) {
      sampleLists = sketch_.getSamples();
    }

    return (sampleLists == null ? null : (T) sampleLists.data[i]);
  }

  public double[] weights() {
    if (sampleLists == null) {
      sampleLists = sketch_.getSamples();
    }

    return (sampleLists == null ? null : sampleLists.weights);
  }

  public double weights(final int i) {
    if (sampleLists == null) {
      sampleLists = sketch_.getSamples();
    }

    return (sampleLists == null ? Double.NaN : sampleLists.weights[i]);
  }

  public static void main(final String[] args) {
    final VarOptItemsSketch<Long> vis = VarOptItemsSketch.getInstance(12);
    for (long i = 1; i <= 20; ++i) {
      vis.update(i, 1.0 * i);
    }

    final VarOptItemsSketchSamples<Long> result = vis.getResult();
    for (VarOptItemsSketchSamples.WeightedSample ws : result) {
      System.out.println(ws.getItem() + ":\t" + ws.getWeight());
    }
  }

}
