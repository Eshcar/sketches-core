package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.quantiles.ThreadAffinity;

public class MWMRHeapUpdateDoublesSketch extends HeapUpdateDoublesSketch {

	private static class ThreadReadContext {
		HeapUpdateDoublesSketch auxiliarySketch_;
		int id_;
	}

	private static class ThreadAffinityContext {
		boolean affinitiyIsSet = false;
	}
//	}

	private static class ThreadWriteContext {
		int sketchCount_;
		int id_;
		HeapUpdateDoublesSketch localSketch_;
	}

	enum ExecutionType {
		PROPOGATOR, SORT_ZIP, MARGESORT_ZIP
	}

	/**
	 * 
	 */

	private final ThreadLocal<ThreadReadContext> threadReadLocal_ = new ThreadLocal<ThreadReadContext>();
//	private final ThreadLocal<ThreadWriteContext> threadWriteLocal_ = new ThreadLocal<ThreadWriteContext>();
	private AtomicLong atomicBitPattern_ = new AtomicLong();
//	private int numberOfWriters_;
	private double[] SharedKBuffers_;
	private AtomicBoolean[] SharedKBuffersPattern_;
	private int levelsNum_;
	private long maxCount_;
	private int numberOfWriters_;
	private HeapUpdateDoublesSketch[] LocalSketchArrays_;
	
	private long debug_ = 0;

	private ExecutorService executorService_;
//	public BackgroundPropogator Propogator_;

	private AtomicInteger WritersID = new AtomicInteger();

	private static final Log LOG = LogFactory.getLog(MWMRHeapUpdateDoublesSketch.class);

	// **CONSTRUCTORS**********************************************************
	public MWMRHeapUpdateDoublesSketch(final int k) {
		super(k); // Checks k
	}

	static MWMRHeapUpdateDoublesSketch newInstance(final int k, int numberOfWriters, int numberOfLevels) {

		final MWMRHeapUpdateDoublesSketch hqs = new MWMRHeapUpdateDoublesSketch(k);
		final int baseBufAlloc = 2 * k; // the min is important

		hqs.putN(0);
		hqs.putCombinedBuffer(new double[baseBufAlloc]);
		hqs.putBaseBufferCount(0);
		hqs.putBitPattern(0); // represent also the base buffer.
		hqs.putMinValue(Double.POSITIVE_INFINITY);
		hqs.putMaxValue(Double.NEGATIVE_INFINITY);
//		hqs.numberOfWriters_ = numberOfWriters;
		
		hqs.levelsNum_ = numberOfLevels;
		hqs.maxCount_ = (int) Math.pow(2, numberOfLevels) * (2 * k); 
		hqs.numberOfWriters_ = numberOfWriters;

		hqs.SharedKBuffers_ = new double[numberOfWriters * k];
		hqs.SharedKBuffersPattern_ = new AtomicBoolean[numberOfWriters];
		hqs.LocalSketchArrays_ = new HeapUpdateDoublesSketch[numberOfWriters];
		for (int i = 0; i < numberOfWriters; i++) {
			hqs.SharedKBuffersPattern_[i] = new AtomicBoolean(false);
			hqs.LocalSketchArrays_[i] = HeapUpdateDoublesSketch.newInstance(k);
			hqs.LocalSketchArrays_[i].putCombinedBuffer(new double[(2 * k) + (numberOfLevels + 1) * k]);
		}

		hqs.executorService_ = Executors.newSingleThreadExecutor();

//		hqs.startPropogator(numberOfWriters);

		return hqs;
	}
	
	public long getDebug_(){
		return debug_;
	}
	
//	public void startPropogator(int numberOfWriters) {
//		Propogator_ = new BackgroundPropogator(numberOfWriters);
//		Propogator_.start();
//	}


	@Override
	public void update(final double dataItem, int MyId) {
		if (Double.isNaN(dataItem)) {
			return;
		}
		// final double maxValue = getMaxValue();
		// final double minValue = getMinValue();
		//
		// if (dataItem > maxValue) {
		// putMaxValue(dataItem);
		// }
		// if (dataItem < minValue) {
		// putMinValue(dataItem);
		// }
		// Do we need a fence here?

//		ThreadWriteContext threadWriteContext = threadWriteLocal_.get();
//		if (threadWriteContext == null) {
//			threadWriteContext = new ThreadWriteContext();
//
//			threadWriteContext.sketchCount_ = 0;
//			threadWriteContext.id_ = WritersID.getAndIncrement();
//			threadWriteContext.localSketch_ = HeapUpdateDoublesSketch.newInstance(k_);
//			threadWriteContext.localSketch_.putCombinedBuffer(new double[(2 * k_) + (levelsNum_ + 1) * k_]);
//
//			if (threadWriteContext.id_ > 0) { // In case you have a warm up!
//				threadWriteContext.id_--;
//			}
//			threadWriteLocal_.set(threadWriteContext);
//		}

		// if (threadWriteContext.id_ != 1) {
		// try {
		// ThreadAffinityContext threadAffinityContext = threadAffinityLocal_.get();
		// if (threadAffinityContext == null) {
		// threadAffinityContext = new ThreadAffinityContext();
		// threadAffinityContext.affinitiyIsSet = true;
		//
		//
		// int core = Affinity_.incrementAndGet();
		//
		// LOG.info("I am a writer and my core is " + ThreadAffinity.currentCore());
		// long mask = 1 << core;
		// ThreadAffinity.setCurrentThreadAffinityMask(mask);
		// LOG.info("I am a writer and my new core is " + ThreadAffinity.currentCore());
		//
		// threadAffinityLocal_.set(threadAffinityContext);
		// }
		// } catch (Exception e) {
		// // TODO: handle exception
		// LOG.info("catched RuntimeException: " + e);
		// }
		// }

		
//		int MyId = (int) (Thread.currentThread().getId() % (long)numberOfWriters_);
//		int MyId =  (id % numberOfWriters_);
//		MyId =  0;
		//TODO: check that every writer gets unique id.
		HeapUpdateDoublesSketch localSketch = LocalSketchArrays_[MyId];
		
		localSketch.update(dataItem);
		
//		if (localSketch.getN() + 1 ==  maxCount_) {
//			prapereJobAndInvokePropogator(MyId, localSketch , dataItem);
//		}else {
//			localSketch.update(dataItem);
//		}
		
		
//		threadWriteContext.sketchCount_++;
//		
//		if (threadWriteContext.sketchCount_ == maxCount_) {
//			prapereJobAndInvokePropogator(threadWriteContext, dataItem); //TODO synchronization is here.
//			threadWriteContext.sketchCount_ = 0;
//		}else {
//			threadWriteContext.localSketch_.update(dataItem);
//		}

	}
	
	private void prapereJobAndInvokePropogator(int id, HeapUpdateDoublesSketch sketch, double dataItem) {
		
		int myLocation = id;
		double[] buffer = sketch.getCombinedBuffer();
		
		buffer[(2 * k_) - 1] = dataItem;
		
		//sort the base buffer
		Arrays.sort(buffer, 0, 2 * k_);
		
		DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(sketch, true);
		
		DoublesSketchAccessor tgtSketchBuf = DoublesSketchAccessor.wrap(sketch, true);
//		final int endingLevel = Util.lowestZeroBitStartingAt(bitPattern, startingLevel);
		tgtSketchBuf.setLevel(levelsNum_);
		
		for (int lvl = 0; lvl < levelsNum_; lvl++) {
//			assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
			
			DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, tgtSketchBuf);
			
			final DoublesSketchAccessor currLevelBuf = tgtSketchBuf.copyAndSetLevel(lvl);
			DoublesUpdateImpl.mergeTwoSizeKBuffers(currLevelBuf, // target level: lvl
					tgtSketchBuf, // target level: endingLevel
					bbAccessor);
		} // end of loop over lower levels
	
		
		//last zip is to to the shared buffer + synchronization with propogator.
		
		while (SharedKBuffersPattern_[myLocation].get() == true){
		  //wait until the location is free 	
		}
		
		FlexDoublesArrayAccessor SharedbufferAccessor = FlexDoublesArrayAccessor.wrap(SharedKBuffers_,
				myLocation  * k_, k_);
		
		DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, SharedbufferAccessor);
		
		SharedKBuffersPattern_[myLocation].set(true);
		BackgroundPropogation job = new BackgroundPropogation(myLocation);
		executorService_.execute(job);
		
		//empty sketch
		
		
		sketch.putBitPattern(0);
		sketch.putN(0);
		sketch.putBaseBufferCount(0);
		
	}

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

		ThreadReadContext threadReadContext = threadReadLocal_.get();
		if (threadReadContext == null) {
			threadReadContext = new ThreadReadContext();

			threadReadContext.auxiliarySketch_ = HeapUpdateDoublesSketch.newInstance(k_);
			threadReadLocal_.set(threadReadContext);
		}

		long bitP1 = getBitPattern(); // bitPattern_;

		int levels = Util.computeTotalLevels(bitP1);
		int spaceNeeded = getRequiredSpace(levels);

		HeapUpdateDoublesSketch auxiliarySketch = threadReadContext.auxiliarySketch_;
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
		long bitP2 = getBitPattern(); //

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

		final DoublesAuxiliary aux = new DoublesAuxiliary(auxiliarySketch);
		return aux.getQuantile(fraction);
	}


	public void resetLocal() {

		threadReadLocal_.set(null);
//		threadWriteLocal_.set(null);

		LOG.info("reset local");
	}

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

	// not in use but I like it.
//	private void resetBB() {
//
//		int len = 2 * k_;
//		getCombinedBuffer()[0] = 0.0;
//
//		for (int i = 1; i < len; i += i) {
//			System.arraycopy(getCombinedBuffer(), 0, getCombinedBuffer(), i, ((len - i) < i) ? (len - i) : i);
//		}
//	}


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

//		public void setAffinity() {
//
//			// try {
//			//
//			// ThreadAffinityContext threadAffinityContext = ds_.threadAffinityLocal_.get();
//			// if (threadAffinityContext == null) {
//			// threadAffinityContext = new ThreadAffinityContext();
//			// threadAffinityContext.affinitiyIsSet = true;
//			//
//			// int core = ds_.Affinity_.getAndIncrement();
//			//
//			// LOG.info("I am a writer and my core is " + ThreadAffinity.currentCore());
//			// long mask = 1 << core;
//			// ThreadAffinity.setCurrentThreadAffinityMask(mask);
//			// LOG.info("I am a writer and my core is " + ThreadAffinity.currentCore());
//			// }
//			// } catch (Exception e) {
//			// // TODO: handle exception
//			// LOG.info("catched RuntimeException: " + e);
//			// }
//
//			try {
//				ThreadAffinityContext threadAffinityContext = threadAffinityLocal_.get();
//
//				// LOG.info("WTF?");
//
//				if (threadAffinityContext == null) {
//					threadAffinityContext = new ThreadAffinityContext();
//					threadAffinityContext.affinitiyIsSet = true;
//
//					int core = Affinity_.incrementAndGet();
//
//					LOG.info("I am a helper and my core is " + ThreadAffinity.currentCore());
//					long mask = 1 << core;
//					ThreadAffinity.setCurrentThreadAffinityMask(mask);
//					LOG.info("I am a helper and my new core is " + ThreadAffinity.currentCore());
//
//					threadAffinityLocal_.set(threadAffinityContext);
//				}
//			} catch (Exception e) {
//				// TODO: handle exception
//				LOG.info("catched RuntimeException: " + e);
//			}
//
//		}
	
	public class BackgroundPropogation implements Runnable{
		
		int location_;
		
		BackgroundPropogation(int location){
			location_ = location;
		}
		
		@Override
		public void run() {
			
			propogate(location_);
			
		}
		
		
		void propogate(int locationToPropogate) {

			//
			long bitPattern = getBitPattern();
			int startingLevel = levelsNum_;

			long newN = getN() + maxCount_;

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
				tgtSketchBuf.putArray(SharedKBuffers_, locationToPropogate * k_, 0, k_);
				SharedKBuffersPattern_[locationToPropogate].set(false);

			} else {
				DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(MWMRHeapUpdateDoublesSketch.this, true);
				FlexDoublesArrayAccessor sharedBuffersAccessor = FlexDoublesArrayAccessor.wrap(SharedKBuffers_,
						locationToPropogate  * k_, k_);
				DoublesSketchAccessor firstLevelBuf = tgtSketchBuf.copyAndSetLevel(startingLevel);
				DoublesUpdateImpl.mergeTwoSizeKBuffers(firstLevelBuf, // target level: lvl
						sharedBuffersAccessor, // target level: endingLevel
						bbAccessor);
				SharedKBuffersPattern_[locationToPropogate].set(false);
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
