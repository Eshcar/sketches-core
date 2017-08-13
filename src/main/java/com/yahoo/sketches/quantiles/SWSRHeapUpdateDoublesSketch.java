/**
 * 
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

import org.omg.CORBA.PRIVATE_MEMBER;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author sashaspiegelman This class supports concurrency between a single
 *         writer that performs Update() and a single reader that performs
 *         getQuntile(). The idea is based an a snapshot - the writer do not use
 *         basebuffer for propagation and the update linearization point is when
 *         the bitPattern is updated. The reader reads the combinedBuffer until
 *         it gets a snapshot. Note that the reader do not need to read the
 *         entire buffer every time it fails to get a snapshot - it is enough to
 *         read the prefix that was changed.
 */

final class SWSRHeapUpdateDoublesSketch extends HeapUpdateDoublesSketch {

	/**
	 * 
	 */

	private DoublesArrayAccessor auxiliaryPropogateArray_;

	// **CONSTRUCTORS**********************************************************
	public SWSRHeapUpdateDoublesSketch(final int k) {
		super(k); // Checks k
	}

	static SWSRHeapUpdateDoublesSketch newInstance(final int k) {
		final SWSRHeapUpdateDoublesSketch hqs = new SWSRHeapUpdateDoublesSketch(k);
		final int baseBufAlloc = 2 * k; // the min is important

		hqs.putN(0);
		hqs.putCombinedBuffer(new double[baseBufAlloc]);
		hqs.putBaseBufferCount(0);
		hqs.putBitPattern(1); // represent also the base buffer.
		hqs.putMinValue(Double.POSITIVE_INFINITY);
		hqs.putMaxValue(Double.NEGATIVE_INFINITY);

		hqs.auxiliaryPropogateArray_ = DoublesArrayAccessor.initialize(2 * k);
		// resetBB();

		return hqs;
	}

	@Override
	public void update(final double dataItem) {
		if (Double.isNaN(dataItem)) {
			return;
		}
		final double maxValue = getMaxValue();
		final double minValue = getMinValue();

		if (dataItem > maxValue) {
			putMaxValue(dataItem);
		}
		if (dataItem < minValue) {
			putMinValue(dataItem);
		}

		// Do we need a fence here?

		// don't increment n_ and baseBufferCount_ yet
		final int curBBCount = getBaseBufferCount(); // baseBufferCount_;
		final int newBBCount = curBBCount + 1;
		final long newN = getN() + 1; // n_ + 1;

		// No need - we allocate base buffer to be 2k at the beginning.

		// final int combBufItemCap = combinedBuffer_.length;
		// if (newBBCount > combBufItemCap) {
		// growBaseBuffer(); // only changes combinedBuffer when it is only a base
		// buffer
		// }

		// put the new item in the base buffer
		getCombinedBuffer()[curBBCount] = dataItem;
		// combinedBuffer_[curBBCount] = dataItem;

		if (newBBCount == k_ << 1) { // Propagate

			// make sure there will be enough space (levels) for the propagation
			final int spaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(k_, newN);
			final int combBufItemCap = getCombinedBufferItemCapacity();

			if (spaceNeeded > combBufItemCap) {
				// copies base buffer plus old levels, adds space for new level
				growCombinedBuffer(combBufItemCap, spaceNeeded);
			}

			// sort only the (full) base buffer via accessor which modifies the underlying
			// base buffer,
			// then use as one of the inputs to propagate-carry
			final DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(this, true);
			bbAccessor.sort();

			// We do not want to dirty the base buffer since the reader may concurrently
			// read it.
			auxiliaryPropogateArray_.putArray(bbAccessor.getArray(0, 2 * k_), 0, 0, 2 * k_);

			// firstZipSize2KBuffer(bbAccessor, scratch2KAcc);

			long legacyBitPattern = getBitPattern() >> 1; // bitPattern_ >> 1;

			final long newBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(0, // starting level
					null, auxiliaryPropogateArray_, true, k_, DoublesSketchAccessor.wrap(this, true), legacyBitPattern);

			assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check
			assert newBitPattern == legacyBitPattern + 1;

			putBitPattern(newBitPattern << 1); // bitPattern_ = newBitPattern << 1;
			putBaseBufferCount(0); // baseBufferCount_ = 0;
			// resetBB();
			putBitPattern(getBitPattern() | 1); // bitPattern_ = bitPattern_ | 1;

		} else {
			// bitPattern unchanged
			putBaseBufferCount(newBBCount); // baseBufferCount_ = newBBCount;
		}
		putN(newN); // n_ = newN;
	}

	// TODO: check the logic.
	@Override
	public double getQuantile(final double fraction) {
		if ((fraction < 0.0) || (fraction > 1.0)) {
			throw new SketchesArgumentException("Fraction cannot be less than zero or greater than 1.0");
		}

		if (fraction == 0.0) {
			return getMinValue();
		} else if (fraction == 1.0) {
			return getMaxValue();
		}

		long bitP1 = getBitPattern(); // bitPattern_;
		int BBCount = getBaseBufferCount(); // baseBufferCount_;

		long legacyBitP1 = bitP1 >> 1;
		int levels = Util.computeTotalLevels(legacyBitP1);
		int spaceNeeded = getRequiredSpace(levels);

		HeapUpdateDoublesSketch auxiliarySketch = HeapUpdateDoublesSketch.newInstance(k_);
		auxiliarySketch.putCombinedBuffer(new double[spaceNeeded]);

		collectOnce(auxiliarySketch, -1, levels);
		auxiliarySketch.putBitPattern(legacyBitP1);

		if ((bitP1 & 1) > 0) {
			auxiliarySketch.putBaseBufferCount(BBCount);
		} else {
			auxiliarySketch.putBaseBufferCount(0);
		}

		long bitP2 = getBitPattern(); // bitPattern_;
		long legacyBitP2 = bitP2 >> 1;

		while (legacyBitP1 != legacyBitP2) {

			levels = Util.computeTotalLevels(legacyBitP2);
			spaceNeeded = getRequiredSpace(levels);

			int auxiliarySketchCap = auxiliarySketch.getCombinedBufferItemCapacity();

			if (spaceNeeded > auxiliarySketchCap) {
				// nothing from previous collect is valid in this case.
				auxiliarySketch.putCombinedBuffer(new double[spaceNeeded]);
			}

			int diffLevels = bitPatternDiff(legacyBitP1, legacyBitP2, levels);
			collect(auxiliarySketch, 0, diffLevels, legacyBitP2);

			legacyBitP1 = legacyBitP2;
			bitP2 = getBitPattern(); // bitPattern_;
			legacyBitP2 = bitP2 >> 1;
			auxiliarySketch.putBaseBufferCount(0);
			auxiliarySketch.putBitPattern(legacyBitP1);
		}

		long n = setNFromBitPattern(legacyBitP1); // does not include the base buffer;
		n += auxiliarySketch.getBaseBufferCount();
		auxiliarySketch.putN(n);

		final DoublesAuxiliary aux = new DoublesAuxiliary(auxiliarySketch);
		return aux.getQuantile(fraction);

	}

	private long setNFromBitPattern(long bits) {

		int levels = Util.computeTotalLevels(bits);

		if (levels == 0) {
			return 0;
		}

		int index = 1 << (levels - 1);
		long weight = power(2, levels);
		long n = 0;

		assert ((bits & index) > 0);

		while (index > 0) {
			if ((bits & index) > 0) {
				n += (k_ * weight);
			}
			weight = weight / 2;
			index = index >> 1;
		}

		// for (int ind = index; ind > 0; ind >> 1) {
		// if ( (bits & ind) > 0 ) {
		// n += (k_ * weight);
		// }
		// weight = weight / 2;
		// }

		return n;
	}

	private long power(int base, int arg) {
		assert (arg >= 0);

		long res;

		if (arg == 0) {
			return 1;
		} else {
			res = base;
			arg--;
		}

		while (arg > 0) {
			res *= base;
			arg--;
		}

		return res;
	}

	private int bitPatternDiff(long bits1, long bits2, int levels) {
		assert (bits2 > bits1);
		assert (levels > 0);
		long index = 1 << (levels - 1);

		for (int i = levels; i > 0; i--) {
			boolean tmp1 = ((bits1 & index) > 0);
			boolean tmp2 = ((bits2 & index) > 0);
			if (tmp1 != tmp2) {
				return levels;
			}
			index = index >> 1;
		}

		assert (false);
		return -1;
	}

	private void collect(HeapUpdateDoublesSketch ds, int stopAtThisLevel, int topLevel, long bits) {
		assert ((bits & (1 << (topLevel - 1))) > 0);

		long index = 1 << (topLevel - 1);

		for (int curLevel = topLevel; curLevel > stopAtThisLevel; curLevel--) {
			if ((bits & index) > 0) {
				int startIndex = (2 * k_) + ((curLevel - 1) * k_);

				System.arraycopy(getCombinedBuffer(), startIndex, ds.getCombinedBuffer(), startIndex, k_);
			}
			index = index >> 1;
		}
	}

	private void collectOnce(HeapUpdateDoublesSketch ds, int startingLevel, int endingLevel) {

		int startIndex;
		int endIndex;

		if (startingLevel == -1) {
			startIndex = 0;
		} else {
			startIndex = (2 * k_) + (startingLevel * k_);
		}

		endIndex = (2 * k_) + (endingLevel * k_);

		int indexNum = endIndex - startIndex;

		System.arraycopy(getCombinedBuffer(), startIndex, ds.getCombinedBuffer(), startIndex, indexNum);

	}

	private int getRequiredSpace(int levels) {

		return (2 + levels) * k_;
	}

	// not in use but I like it.
	private void resetBB() {

		int len = 2 * k_;
		getCombinedBuffer()[0] = 0.0;

		for (int i = 1; i < len; i += i) {
			System.arraycopy(getCombinedBuffer(), 0, getCombinedBuffer(), i, ((len - i) < i) ? (len - i) : i);
		}
	}

	@Override
	public void reset() {
		final int baseBufAlloc = 2 * k_; 

		this.putN(0);
		this.putCombinedBuffer(new double[baseBufAlloc]);
		this.putBaseBufferCount(0);
		this.putBitPattern(1); // represent also the base buffer.
		this.putMinValue(Double.POSITIVE_INFINITY);
		this.putMaxValue(Double.NEGATIVE_INFINITY);

	}

}
