package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import com.yahoo.sketches.SketchesArgumentException;

public class MWMRHeapUpdateDoublesSketch extends HeapUpdateDoublesSketch {



	/**
	 * 
	 */

	private AtomicLong atomicBitPattern_ = new AtomicLong();
	private AtomicInteger numberOfWriters_ = new AtomicInteger(0);
//	private int levelsNum_ = 0;
//	private long maxCount_;

	private long debug_ = 0;

	private ExecutorService executorService_;

	private static final Log LOG = LogFactory.getLog(MWMRHeapUpdateDoublesSketch.class);

	// **CONSTRUCTORS**********************************************************
	public MWMRHeapUpdateDoublesSketch(final int k) {
		super(k); // Checks k
	}
	
	
	


	static MWMRHeapUpdateDoublesSketch newInstance(final int k) {

		final MWMRHeapUpdateDoublesSketch hqs = new MWMRHeapUpdateDoublesSketch(k);
		final int baseBufAlloc = 2 * k; // the min is important

		hqs.putN(0);
		hqs.putCombinedBuffer(new double[baseBufAlloc]);
		hqs.putBaseBufferCount(0);
		hqs.putBitPattern(0); // represent also the base buffer.
		hqs.putMinValue(Double.POSITIVE_INFINITY);
		hqs.putMaxValue(Double.NEGATIVE_INFINITY);
		
//		hqs.levelsNum_ = numberOfLevels;
//		hqs.maxCount_ = (int) Math.pow(2, numberOfLevels) * (2 * k);
		hqs.executorService_ = Executors.newSingleThreadExecutor();

		return hqs;
	}
	
	public long getDebug_(){
		return debug_;
	}
	
	public int getNumberOfWriters_() {
		return numberOfWriters_.get();
	}
	
	public int incrementAndGetNumberOfWriters_() {
		return numberOfWriters_.incrementAndGet();
	}
	
	public void decrementNumberOfWriters_() {
		 numberOfWriters_.decrementAndGet();
	}
	
	
	public void prpogate(FlexDoublesArrayAccessor buffer, int level, AtomicBoolean full_) {
		BackgroundPropogation job = new BackgroundPropogation(buffer, level, full_);
		executorService_.execute(job);
	}

	
	public void getSnapshot(HeapUpdateDoublesSketch auxiliarySketch) {
		
		long bitP1 = getBitPattern(); // bitPattern_;

		int levels = Util.computeTotalLevels(bitP1);
		int spaceNeeded = getRequiredSpace(levels);

		int currBufferSize = auxiliarySketch.getCombinedBufferItemCapacity();
		if (spaceNeeded > currBufferSize) {
			auxiliarySketch.putCombinedBuffer(new double[spaceNeeded]);
		}

		int diffLevels;

		if (levels > 0) {
			long auxiliaryBitPattern = auxiliarySketch.getBitPattern();

			if (bitP1 != auxiliaryBitPattern) {
				diffLevels = bitPatternDiff(auxiliaryBitPattern, bitP1, levels);
				collect(auxiliarySketch, 0, diffLevels, bitP1);
				auxiliarySketch.putBitPattern(bitP1);
			}
		}
		long bitP2 = getBitPattern(); 

		while (bitP1 != bitP2) {

			levels = Util.computeTotalLevels(bitP2);
			spaceNeeded = getRequiredSpace(levels);

			currBufferSize = auxiliarySketch.getCombinedBufferItemCapacity();

			if (spaceNeeded > currBufferSize) {
				// nothing from previous collect is valid in this case.
				auxiliarySketch.putCombinedBuffer(new double[spaceNeeded]);
			}

			diffLevels = bitPatternDiff(bitP1, bitP2, levels);
			collect(auxiliarySketch, 0, diffLevels, bitP2);

			bitP1 = bitP2;
			bitP2 = getBitPattern(); // bitPattern_;

			auxiliarySketch.putBitPattern(bitP1);
		}

		long n = setNFromBitPattern(bitP1);
		auxiliarySketch.putN(n);
		
	}

//	@Override
//	public double getQuantile(final double fraction) {
//
//
//		if ((fraction < 0.0) || (fraction > 1.0)) {
//			throw new SketchesArgumentException("Fraction cannot be less than zero or greater than 1.0");
//		}
//
////		if (fraction == 0.0) {
////			return getMinValue();
////		} else if (fraction == 1.0) {
////			return getMaxValue();
////		}
//
//		ThreadReadContext threadReadContext = threadReadLocal_.get();
//		if (threadReadContext == null) {
//			threadReadContext = new ThreadReadContext();
//
//			threadReadContext.auxiliarySketch_ = HeapUpdateDoublesSketch.newInstance(k_);
//			threadReadLocal_.set(threadReadContext);
//		}
//
//		long bitP1 = getBitPattern(); // bitPattern_;
//
//		int levels = Util.computeTotalLevels(bitP1);
//		int spaceNeeded = getRequiredSpace(levels);
//
//		HeapUpdateDoublesSketch auxiliarySketch = threadReadContext.auxiliarySketch_;
//		int currBufferSize = auxiliarySketch.getCombinedBufferItemCapacity();
//		if (spaceNeeded > currBufferSize) {
//			auxiliarySketch.putCombinedBuffer(new double[spaceNeeded]);
//		}
//
//		int diffLevels;
//
//		if (levels > 0) {
//			long auxiliaryBitPattern = auxiliarySketch.getBitPattern();
//
//			if (bitP1 != auxiliaryBitPattern) {
//				diffLevels = bitPatternDiff(auxiliaryBitPattern, bitP1, levels);
//				collect(auxiliarySketch, 0, diffLevels, bitP1);
//				auxiliarySketch.putBitPattern(bitP1);
//			}
//		}
//		long bitP2 = getBitPattern(); //
//
//		while (bitP1 != bitP2) {
//
//			levels = Util.computeTotalLevels(bitP2);
//			spaceNeeded = getRequiredSpace(levels);
//
//			currBufferSize = auxiliarySketch.getCombinedBufferItemCapacity();
//
//			if (spaceNeeded > currBufferSize) {
//				// nothing from previous collect is valid in this case.
//				auxiliarySketch.putCombinedBuffer(new double[spaceNeeded]);
//			}
//
//			diffLevels = bitPatternDiff(bitP1, bitP2, levels);
//			collect(auxiliarySketch, 0, diffLevels, bitP2);
//
//			bitP1 = bitP2;
//			bitP2 = getBitPattern(); // bitPattern_;
//
//			auxiliarySketch.putBitPattern(bitP1);
//		}
//
//		long n = setNFromBitPattern(bitP1);
//		auxiliarySketch.putN(n);
//
//		final DoublesAuxiliary aux = new DoublesAuxiliary(auxiliarySketch);
//		return aux.getQuantile(fraction);
//	}


//	public void resetLocal() {
//
//
//		threadReadLocal_.set(null);
////		threadWriteLocal_.set(null);
//
//		LOG.info("reset local");
//	}

	private long setNFromBitPattern(long bits) {

		int levels = Util.computeTotalLevels(bits);

		if (levels == 0) {
			return 0;
		}

		int index = 1 << (levels - 1);
		long weight = (long) Math.pow(2, levels);
		long n = 0;

		assert ((bits & index) > 0);

		while (index > 0) {
			if ((bits & index) > 0) {
				n += (k_ * weight);
			}
			weight = weight / 2;
			index = index >> 1;
		}


		return n;
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


	private int getRequiredSpace(int levels) {

		return (2 + levels) * k_;
	}


	public void clean() {

		
//		Propogator_.setRun_(false);
		
		try {
//			Propogator_.join();
			executorService_.shutdown();
		}catch (Exception e) {
			LOG.info("exception: " + e);
		}
	}

	@Override
	void putBitPattern(final long bitPattern) {

		atomicBitPattern_.set(bitPattern);
	}

	@Override
	long getBitPattern() {
		return atomicBitPattern_.get();
	}

	public class BackgroundPropogation implements Runnable{
		
		FlexDoublesArrayAccessor buffer_;
		AtomicBoolean full_;
		int levelsNum_;
//		private final Log LOG = LogFactory.getLog(BackgroundPropogation.class);
		
		BackgroundPropogation(FlexDoublesArrayAccessor buffer, int levelsNum,  AtomicBoolean full){
			buffer_ = buffer;
			full_ = full;
			levelsNum_ = levelsNum;
		}
		
		@Override
		public void run() {
			
			propogate(buffer_, full_);
		}
		
		
		void propogate(FlexDoublesArrayAccessor buffer, AtomicBoolean full) {

			//
			long bitPattern = getBitPattern();
			int startingLevel = levelsNum_;

			long newN = getN() + (int) Math.pow(2, levelsNum_) * (2 * k_);

			// make sure there will be enough space (levels) for the propagation
			final int spaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(k_, newN);
			final int combBufItemCap = getCombinedBufferItemCapacity();

			if (spaceNeeded > combBufItemCap) {
				// copies base buffer plus old levels, adds space for new level
				growCombinedBuffer(combBufItemCap, spaceNeeded);
			}

			
			DoublesSketchAccessor tgtSketchBuf = DoublesSketchAccessor.wrap(MWMRHeapUpdateDoublesSketch.this, true);
			final int endingLevel = Util.lowestZeroBitStartingAt(bitPattern, startingLevel);
			tgtSketchBuf.setLevel(endingLevel);

			if (endingLevel == startingLevel) {
				tgtSketchBuf.putArray(buffer.getBuffer(), 0, 0, k_);
				full.set(false);

			} else {
				DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(MWMRHeapUpdateDoublesSketch.this, true);
				DoublesSketchAccessor firstLevelBuf = tgtSketchBuf.copyAndSetLevel(startingLevel);
				DoublesUpdateImpl.mergeTwoSizeKBuffers(firstLevelBuf, // target level: lvl
						buffer, // target level: endingLevel
						bbAccessor);
				full.set(false);
				DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, tgtSketchBuf);

				for (int lvl = startingLevel + 1; lvl < endingLevel; lvl++) {
//					assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
					final DoublesSketchAccessor currLevelBuf = tgtSketchBuf.copyAndSetLevel(lvl);
					DoublesUpdateImpl.mergeTwoSizeKBuffers(currLevelBuf, // target level: lvl
							tgtSketchBuf, // target level: endingLevel
							bbAccessor);
					DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, tgtSketchBuf);
				} // end of loop over lower levels
			}

			
			long newBitPattern = bitPattern + (1L << startingLevel);
			assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check

			putBitPattern(newBitPattern);
			putN(newN);

		}
	}
}
