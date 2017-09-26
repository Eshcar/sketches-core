package com.yahoo.sketches.quantiles;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.sketches.SketchesArgumentException;


public class Contex {

	private MWMRHeapUpdateDoublesSketch ds_;
	private HeapUpdateDoublesSketch localSketch_;
	private long maxCount_;
	private AtomicBoolean full_ = new AtomicBoolean(false);
	private FlexDoublesArrayAccessor propogationBufferAccessor_;
	private int k_;
	private int levelsNum_;
	
	private HeapUpdateDoublesSketch auxiliaryReadSketch_;

	public Contex(MWMRHeapUpdateDoublesSketch ds) {
		ds_ = ds;
		k_ = ds.getK();
		levelsNum_ = ds.getLevelsNum_();
		localSketch_ = HeapUpdateDoublesSketch.newInstance(k_);
		localSketch_.putCombinedBuffer(new double[(2 * k_) + (levelsNum_ + 1) * k_]);
		maxCount_ = (long) Math.pow(2, levelsNum_) * (2 * k_);
		propogationBufferAccessor_ = FlexDoublesArrayAccessor.wrap(new double[k_], 0, k_);
		auxiliaryReadSketch_ = HeapUpdateDoublesSketch.newInstance(k_);
	}

	public void update(double dataItem) {
		
		if (localSketch_.getN() + 1 ==  maxCount_) {
			propogateToSharedSketch(dataItem);
		}else {
			localSketch_.update(dataItem);
		}
	}
	
	
	public double getQuantile(final double fraction) {
		
		if ((fraction < 0.0) || (fraction > 1.0)) {
			throw new SketchesArgumentException("Fraction cannot be less than zero or greater than 1.0");
		}
		
		ds_.getSnapshot(auxiliaryReadSketch_);
		
		final DoublesAuxiliary aux = new DoublesAuxiliary(auxiliaryReadSketch_);
		return aux.getQuantile(fraction);
		
	}

	private void propogateToSharedSketch(double dataItem) {

		double[] buffer = localSketch_.getCombinedBuffer();

		buffer[(2 * k_) - 1] = dataItem;

		// sort the base buffer
		Arrays.sort(buffer, 0, 2 * k_);

		DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(localSketch_, true);

		DoublesSketchAccessor tgtSketchBuf = DoublesSketchAccessor.wrap(localSketch_, true);
		// final int endingLevel = Util.lowestZeroBitStartingAt(bitPattern,
		// startingLevel);
		tgtSketchBuf.setLevel(levelsNum_);

		for (int lvl = 0; lvl < levelsNum_; lvl++) {
			// assert (bitPattern & (1L << lvl)) > 0; // internal consistency check

			DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, tgtSketchBuf);

			final DoublesSketchAccessor currLevelBuf = tgtSketchBuf.copyAndSetLevel(lvl);
			DoublesUpdateImpl.mergeTwoSizeKBuffers(currLevelBuf, // target level: lvl
					tgtSketchBuf, // target level: endingLevel
					bbAccessor);
		} // end of loop over lower levels

		// last zip is to to the shared buffer + synchronization with propogator.

		while (full_.get() == true) {
			// wait until the location is free
		}

//		FlexDoublesArrayAccessor SharedbufferAccessor = FlexDoublesArrayAccessor.wrap(SharedKBuffers_, myLocation * k_,
//				k_);

		DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, propogationBufferAccessor_);
//		DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, SharedbufferAccessor);

		full_.set(true);
		
		
		//TODO:
		ds_.prpogate(propogationBufferAccessor_, full_);
//		BackgroundPropogation job = new BackgroundPropogation(propogationBufferAccessor_, full_);
//		executorService_.execute(job);

		// empty sketch

		localSketch_.putBitPattern(0);
		localSketch_.putN(0);
		localSketch_.putBaseBufferCount(0);

	}

}
