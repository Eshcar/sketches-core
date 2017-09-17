package com.yahoo.sketches.quantiles;

import java.util.Arrays;

public class FlexDoublesArrayAccessor extends DoublesBufferAccessor {

	private int numItems_;
	private double[] buffer_;
	private int startIndex_; // included

	public FlexDoublesArrayAccessor(final double[] buffer, int startIndex, int numItems) {
		buffer_ = buffer;
		startIndex_ = startIndex;
		numItems_ = numItems;
	}

	static FlexDoublesArrayAccessor wrap(final double[] buffer, int startIndex, int numItems) {
		return new FlexDoublesArrayAccessor(buffer, startIndex, numItems);
	}

	@Override
	double get(int index) {
		assert index >= 0 && index < numItems_;
		return buffer_[startIndex_ + index];
	}

	@Override
	double set(int index, double value) {
		assert index >= 0 && index < numItems_;
		final double retValue = buffer_[startIndex_ + index];
		buffer_[startIndex_ + index] = value;
		return retValue;
	}

	@Override
	int numItems() {
		return numItems_;
	}

	@Override
	double[] getArray(final int fromIdx, final int numItems) {
		assert(numItems_ >= numItems);
		return Arrays.copyOfRange(buffer_, startIndex_ + fromIdx, startIndex_ + fromIdx + numItems);
	}

	@Override
	void putArray(final double[] srcArray, final int srcIndex, final int dstIndex, final int numItems) {
		assert(numItems_ >= numItems);
		System.arraycopy(srcArray, srcIndex, buffer_, startIndex_ + dstIndex, numItems);
	}

}
