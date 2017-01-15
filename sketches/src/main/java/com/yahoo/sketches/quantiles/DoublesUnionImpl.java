/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.quantiles.DoublesUtil.copyToHeap;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.FLAGS_BYTE;

import com.yahoo.memory.Memory;

/**
 * Union operation for on-heap.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public final class DoublesUnionImpl extends DoublesUnion {
  private int maxK_;
  private DoublesSketch gadget_ = null;

  private DoublesUnionImpl(final int maxK) {
    maxK_ = maxK;
  }

  /**
   * Returns a empty Heap DoublesUnion object.
   * @param maxK determines the accuracy and size of the union and is a maximum value.
   * The effective <i>k</i> can be smaller due to unions with smaller <i>k</i> sketches.
   * It is recommended that <i>maxK</i> be a power of 2 to enable unioning of sketches with
   * different values of <i>k</i>.

   */
  static DoublesUnionImpl heapInstance(final int maxK) {
    final DoublesUnionImpl union = new DoublesUnionImpl(maxK);
    return union;
  }

  /**
   * Returns a empty DoublesUnion object that refers to the given direct, off-heap Memory,
   * which will be initialized to the empty state.
   *
   * @param maxK determines the accuracy and size of the union and is a maximum value.
   * The effective <i>k</i> can be smaller due to unions with smaller <i>k</i> sketches.
   * It is recommended that <i>maxK</i> be a power of 2 to enable unioning of sketches with
   * different values of <i>k</i>.
   * @param dstMem the Memory to be used by the sketch
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl directInstance(final int maxK, final Memory dstMem) {
    final DirectDoublesSketch sketch = DirectDoublesSketch.newInstance(maxK, dstMem);
    final DoublesUnionImpl union = new DoublesUnionImpl(maxK);
    union.maxK_ = maxK;
    union.gadget_ = sketch;
    return union;
  }

  /**
   * Returns a Heap DoublesUnion object that has been initialized with the data from the given
   * sketch.
   *
   * @param sketch A DoublesSketch to be used as a source of data only and will not be modified.
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl heapifyInstance(final DoublesSketch sketch) {
    final int k = sketch.getK();
    final DoublesUnionImpl union = new DoublesUnionImpl(k);
    union.maxK_ = k;
    union.gadget_ = copyToHeap(sketch);
    return union;
  }

  /**
   * Returns a Heap DoublesUnion object that has been initialized with the data from the given
   * Memory image of a DoublesSketch. The srcMem object will not be modified and a reference to
   * it is not retained. The <i>maxK</i> of the resulting union will be that obtained from
   * the sketch Memory image.
   *
   * @param srcMem a Memory image of a quantiles DoublesSketch
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl heapifyInstance(final Memory srcMem) {
    final long n = srcMem.getLong(PreambleUtil.N_LONG);
    final int k = srcMem.getShort(PreambleUtil.K_SHORT) & 0xFFFF;
    final HeapDoublesSketch sketch = (n == 0)
        ? HeapDoublesSketch.newInstance(k)
        : HeapDoublesSketch.heapifyInstance(srcMem);
    final DoublesUnionImpl union = new DoublesUnionImpl(k);
    union.maxK_ = k;
    union.gadget_ = sketch;
    return union;
  }

  /**
   * Returns a Union object that wraps off-heap data structure of the given memory image of
   * a non-compact DoublesSketch. The data structures of the Union remain off-heap.
   *
   * @param mem A memory image of a non-compact DoublesSketch to be used as the data
   * structure for the union and will be modified.
   * @return a Union object
   */
  static DoublesUnionImpl wrapInstance(final Memory mem) {
    final DirectDoublesSketch sketch = DirectDoublesSketch.wrapInstance(mem);
    final int k = sketch.getK();
    final DoublesUnionImpl union = new DoublesUnionImpl(k);
    union.maxK_ = k;
    union.gadget_ = sketch;
    return union;
  }

  @Override
  public boolean isEmpty() {
    return (gadget_ == null) ? true : gadget_.isEmpty();
  }

  @Override
  public boolean isDirect() {
    return (gadget_ == null) ? false : gadget_.isDirect();
  }

  @Override
  public int getMaxK() {
    return maxK_;
  }

  @Override
  public int getEffectiveK() {
    return (gadget_ != null) ? gadget_.getK() : maxK_;
  }

  @Override
  public void update(final DoublesSketch sketchIn) {
    gadget_ = updateLogic(maxK_, gadget_, sketchIn);
  }

  @Override
  public void update(final Memory mem) {
    gadget_ = updateLogic(maxK_, gadget_, HeapDoublesSketch.heapifyInstance(mem));
  }

  @Override
  public void update(final double dataItem) {
    if (gadget_ == null) {
      gadget_ = HeapDoublesSketch.newInstance(maxK_);
    }
    gadget_.update(dataItem);
  }

  @Override
  public DoublesSketch getResult() {
    if (gadget_ == null) {
      return HeapDoublesSketch.newInstance(maxK_);
    }
    return DoublesUtil.copyToHeap(gadget_); //can't have any externally owned handles.
  }

  @Override
  public DoublesSketch getResultAndReset() {
    if (gadget_ == null) { return null; } //Intentionally return null here for speed.
    final DoublesSketch ds = gadget_;
    gadget_ = null;
    return ds;
  }

  @Override
  public void reset() {
    gadget_ = null;
  }

  @Override
  public byte[] toByteArray() {
    if (gadget_ == null) {
      final HeapDoublesSketch sketch = HeapDoublesSketch.newInstance(maxK_);
      return DoublesByteArrayImpl.toByteArray(sketch, true, false);
    }
    return DoublesByteArrayImpl.toByteArray(gadget_, true, false);
  }

  @Override
  public String toString() {
    return toString(true, false);
  }

  @Override
  public String toString(final boolean sketchSummary, final boolean dataDetail) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = this.getClass().getSimpleName();
    final int k = this.getMaxK();
    final String kStr = String.format("%,d", k);
    sb.append(Util.LS).append("### Quantiles ").append(thisSimpleName).append(LS);
    sb.append("   maxK                         : ").append(kStr);
    if (gadget_ == null) {
      sb.append(HeapDoublesSketch.newInstance(maxK_).toString());
      return sb.toString();
    }
    sb.append(gadget_.toString(sketchSummary, dataDetail));
    return sb.toString();
  }

  //@formatter:off
  @SuppressWarnings("null")
  static DoublesSketch updateLogic(final int myMaxK, final DoublesSketch myQS,
      final DoublesSketch other) {
    int sw1 = ((myQS  == null) ? 0 :  myQS.isEmpty() ? 4 : 8);
    sw1 |=    ((other == null) ? 0 : other.isEmpty() ? 1 : 2);
    int outCase = 0; //0=null, 1=NOOP, 2=copy, 3=merge
    switch (sw1) {
      case 0:  outCase = 0; break; //myQS = null,  other = null ; return null
      case 1:  outCase = 4; break; //myQS = null,  other = empty; create empty-heap(myMaxK)
      case 2:  outCase = 2; break; //myQS = null,  other = valid; stream or downsample to myMaxK
      case 4:  outCase = 1; break; //myQS = empty, other = null ; no-op
      case 5:  outCase = 1; break; //myQS = empty, other = empty; no-op
      case 6:  outCase = 3; break; //myQS = empty, other = valid; merge
      case 8:  outCase = 1; break; //myQS = valid, other = null ; no-op
      case 9:  outCase = 1; break; //myQS = valid, other = empty: no-op
      case 10: outCase = 3; break; //myQS = valid, other = valid; merge
      //default: //This cannot happen and cannot be tested
    }
    DoublesSketch ret = null;
    switch (outCase) {
      case 0: ret = null; break; //retun null
      case 1: ret = myQS; break; //no-op
      case 2: { //myQS = null,  other = valid; stream or downsample to myMaxK
        if (!other.isEstimationMode()) { //other is exact, stream items in
          ret = HeapDoublesSketch.newInstance(myMaxK);
          final int otherCnt = other.getBaseBufferCount();
          final double[] combBuf = other.getCombinedBuffer();
          for (int i = 0; i < otherCnt; i++) {
            ret.update(combBuf[i]);
          }
        }
        else { //myQS = null, other is est mode
          ret = (myMaxK < other.getK())
              ? other.downSample(other, myMaxK, null) //null mem
              : DoublesUtil.copyToHeap(other); //copy required because caller has handle
        }
        break;
      }
      case 3: { //myQS = empty/valid, other = valid; merge
        if (!other.isEstimationMode()) { //other is exact, stream items in
          ret = myQS;
          final int otherCnt = other.getBaseBufferCount();
          final double[] combBuf = other.getCombinedBuffer();
          for (int i = 0; i < otherCnt; i++) {
            ret.update(combBuf[i]);
          }
        }
        else { //myQS = empty/valid, other = valid and in est mode
          if (myQS.getK() <= other.getK()) { //I am smaller or equal, thus the target
            mergeInto(other, myQS);
            ret = myQS;
          }
          else { //Bigger: myQS.getK() > other.getK(), must effectively downsize me
            if (myQS.isEmpty()) {
              if (myQS.isDirect()) {
                final Memory mem = myQS.getMemory();
                other.putMemory(mem, true, false); //ordered, not compact
                ret = DoublesSketch.wrap(mem);
              } else { //myQS is empty and on heap
                ret = DoublesUtil.copyToHeap(other);
              }
            }
            else { //Not Empty: I have data, downsample to tmp
              final DoublesSketch tmp = DoublesSketch.builder().build(other.getK());

              DoublesMergeImpl.downSamplingMergeInto(myQS, tmp);
              ret = (myQS.isDirect())
                  ? DoublesSketch.builder().initMemory(myQS.getMemory()).build(other.getK())
                  : DoublesSketch.builder().build(other.getK());

              mergeInto(tmp, ret);
              mergeInto(other, ret);
            }
          }
        }
        break;
      }
      case 4: { //myQS = null,  other = empty; create empty-heap(myMaxK)
        ret = HeapDoublesSketch.newInstance(myMaxK);
        break;
      }
      //default: //This cannot happen and cannot be tested
    }
    return ret;
  }
  //@formatter:on

  /**
   * Merges the source sketch into the target sketch that can have a smaller value of K.
   * However, it is required that the ratio of the two K values be a power of 2.
   * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
   * The source is not modified.
   *
   * <p>Note: It is easy to prove that the following simplified code which launches multiple waves of
   * carry propagation does exactly the same amount of merging work (including the work of
   * allocating fresh buffers) as the more complicated and seemingly more efficient approach that
   * tracks a single carry propagation wave through both sketches.
   *
   * <p>This simplified code probably does do slightly more "outer loop" work, but I am pretty
   * sure that even that is within a constant factor of the more complicated code, plus the
   * total amount of "outer loop" work is at least a factor of K smaller than the total amount of
   * merging work, which is identical in the two approaches.
   *
   * <p>Note: a two-way merge that doesn't modify either of its two inputs could be implemented
   * by making a deep copy of the larger sketch and then merging the smaller one into it.
   * However, it was decided not to do this.
   *
   * @param src The source sketch
   * @param tgt The target sketch
   */
  static void mergeInto(final DoublesSketch src, final DoublesSketch tgt) {
    final int srcK = src.getK();
    final int tgtK = tgt.getK();
    final long srcN = src.getN();
    final long tgtN = tgt.getN();

    if (srcK != tgtK) {
      DoublesMergeImpl.downSamplingMergeInto(src, tgt);
      return;
    }

    final double[] srcCombBuf = src.getCombinedBuffer();
    final long nFinal = tgtN + srcN;

    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update(srcCombBuf[i]);
    }

    final int spaceNeeded = DoublesUpdateImpl.maybeGrowLevels(tgtK, nFinal);
    final int tgtCombBufCap = tgt.getCombinedBufferItemCapacity();
    final double[] newTgtCombBuf;
    if (spaceNeeded > tgtCombBufCap) { //copies base buffer plus current levels
      newTgtCombBuf = tgt.growCombinedBuffer(tgtCombBufCap, spaceNeeded);
    } else {
      newTgtCombBuf = tgt.getCombinedBuffer();
    }
    final double[] scratch2KBuf = new double[2 * tgtK];

    long srcBitPattern = src.getBitPattern();
    assert srcBitPattern == (srcN / (2L * srcK));

    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        final long newTgtBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
            srcLvl,
            srcCombBuf, ((2 + srcLvl) * tgtK),
            scratch2KBuf, 0,
            false,
            tgtK,
            newTgtCombBuf,
            tgt.getBitPattern()
        );
        tgt.putBitPattern(newTgtBitPattern);
        // won't update tgt.n_ until the very end
      }
    }

    if (tgt.isDirect() && (nFinal > 0)) {
      tgt.putCombinedBuffer(newTgtCombBuf);
      final Memory mem = tgt.getMemory();
      mem.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    }

    tgt.putN(nFinal);

    assert tgt.getN() / (2 * tgtK) == tgt.getBitPattern(); // internal consistency check

    final double srcMax = src.getMaxValue();
    final double srcMin = src.getMinValue();
    final double tgtMax = tgt.getMaxValue();
    final double tgtMin = tgt.getMinValue();
    tgt.putMaxValue(Math.max(srcMax, tgtMax));
    tgt.putMinValue(Math.min(srcMin, tgtMin));
  }

}
