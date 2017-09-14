package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

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

//	private static class ThreadHelperContext {
//		DoublesArrayAccessor buffer2k_;
//	}

	private static class ThreadWriteContext {
//		double[] buffer_;
		int index_;
//		int NextBaseTreeNodeNum_;
		int id_;
//		boolean steal_;
	}

	enum ExecutionType {
		PROPOGATOR, SORT_ZIP, MARGESORT_ZIP
	}

	/**
	 * 
	 */

	private final ThreadLocal<ThreadReadContext> threadReadLocal_ = new ThreadLocal<ThreadReadContext>();
	private final ThreadLocal<ThreadWriteContext> threadWriteLocal_ = new ThreadLocal<ThreadWriteContext>();
	// public final ThreadLocal<ThreadAffinityContext> threadAffinityLocal_ = new
	// ThreadLocal<ThreadAffinityContext>();
	// private final ThreadLocal<ThreadHelperContext> threadHelperContext_ = new
	// ThreadLocal<ThreadHelperContext>();
	private AtomicLong atomicBitPattern_ = new AtomicLong();
	// private AtomicInteger atomicBaseBufferCount_ = new AtomicInteger();
	// private ExecutorService executorService_;
	// private int numberOfThreads_;
	// private int numberOfTreeLevels_;
	private int numberOfWriters_;
	private double[] BaseBuffers_;
	private AtomicBoolean[] BaseBufferPatterns_;
	// private AtomicInteger[] treeBaseBitPattern_;
	// private AtomicBoolean[] treeBaselock_;
	// private DoublesArrayAccessor treeBuffer_;
	// private int[] treeBitPattern_;
	// private int[] treeReadyBitPattern_;
	// private AtomicInteger PropogationLock_;

	public BackgroundPropogator Propogator_;

	private AtomicInteger WritersID = new AtomicInteger();
	// public AtomicInteger Affinity_ = new AtomicInteger();

	// public int debug_ = 0;
	//
	// @Override
	// public int getDebug_() {
	// return debug_;
	// }

	private static final Log LOG = LogFactory.getLog(MWMRHeapUpdateDoublesSketch.class);

	// **CONSTRUCTORS**********************************************************
	public MWMRHeapUpdateDoublesSketch(final int k) {
		super(k); // Checks k
	}

	static MWMRHeapUpdateDoublesSketch newInstance(final int k, int numberOfWriters) {

		final MWMRHeapUpdateDoublesSketch hqs = new MWMRHeapUpdateDoublesSketch(k);
		final int baseBufAlloc = 2 * k; // the min is important

		hqs.putN(0);
		hqs.putCombinedBuffer(new double[baseBufAlloc]);
		hqs.putBaseBufferCount(0);
		hqs.putBitPattern(0); // represent also the base buffer.
		hqs.putMinValue(Double.POSITIVE_INFINITY);
		hqs.putMaxValue(Double.NEGATIVE_INFINITY);
		// hqs.numberOfThreads_ = numberOfThreads;
		// hqs.numberOfTreeLevels_ = numberOfTreeLevels;
		hqs.numberOfWriters_ = numberOfWriters;
		// hqs.executorService_ = Executors.newSingleThreadExecutor();
		// hqs.executorService_ = Executors.newFixedThreadPool(numberOfThreads);

		hqs.BaseBuffers_ = new double[2 * numberOfWriters * 2 * k];
		hqs.BaseBufferPatterns_ = new AtomicBoolean[2 * numberOfWriters];
		for (int i = 0; i < (2 * numberOfWriters); i++) {
			hqs.BaseBufferPatterns_[i] = new AtomicBoolean(false);
		}

		// TODO invoke the propogator.

		hqs.startPropogator(numberOfWriters);

		// hqs.BaseBuffer_ = DoublesArrayAccessor.initialize(2 * numberOfWriters * 2 *
		// k);

		// int numberOfLeaves = numberOfLeaves(numberOfTreeLevels);
		// hqs.TreeBaseBuffer_ = DoublesArrayAccessor.initialize(numberOfLeaves * 2 *
		// k);

		// hqs.treeBaseBitPattern_ = new AtomicInteger[numberOfLeaves];
		// hqs.treeBaselock_ = new AtomicBoolean[numberOfLeaves];
		// for (int i = 0; i < numberOfLeaves; i++) {
		// hqs.treeBaseBitPattern_[i] = new AtomicInteger();
		// hqs.treeBaselock_[i] = new AtomicBoolean(false);
		// }
		//
		// int numberOfTreeNodes = numberOfTreeNodes(numberOfTreeLevels);
		// hqs.treeBuffer_ = DoublesArrayAccessor.initialize(numberOfTreeNodes * k);

		// hqs.treeBitPattern_ = new AtomicInteger[numberOfTreeNodes];
		// for (int i = 0; i < numberOfTreeNodes; i++) {
		// hqs.treeBitPattern_[i] = new AtomicInteger();
		// }

		// hqs.treeReadyBitPattern_ = new AtomicInteger[numberOfTreeNodes];
		// for (int i = 0; i < numberOfTreeNodes; i++) {
		// hqs.treeReadyBitPattern_[i] = new AtomicInteger();
		// }

		// hqs.PropogationLock_ = new AtomicInteger();

		return hqs;
	}
	
	public void startPropogator(int numberOfWriters) {
		Propogator_ = new BackgroundPropogator(numberOfWriters);
		Propogator_.start();
	}

	// static int numberOfLeaves(int numberOfTreeLevels) {
	// return (int) Math.pow(2, (numberOfTreeLevels - 1));
	// }

	// static int numberOfTreeNodes(int numberOfTreeLevels) {
	// return (int) (Math.pow(2, numberOfTreeLevels) - 1);
	// }

	@Override
	public void update(final double dataItem) {
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

		ThreadWriteContext threadWriteContext = threadWriteLocal_.get();
		if (threadWriteContext == null) {
			threadWriteContext = new ThreadWriteContext();

			threadWriteContext.index_ = 0;
			// threadWriteContext.buffer_ = new double[2 * k_];
			threadWriteContext.id_ = WritersID.incrementAndGet();
			// threadWriteContext.steal_ = false;

			// LOG.info("id = " + threadWriteContext.id_);

			if (threadWriteContext.id_ > 1) { // In case you have a warm up!
				threadWriteContext.id_--;
			}
			threadWriteLocal_.set(threadWriteContext);
		}

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

		int myLocation = (threadWriteContext.id_ - 1) * 2;

		if (threadWriteContext.index_ == 0) {
			while (BaseBufferPatterns_[myLocation].get()) {
			} // TODO: possible bottleneck
		}

		if (threadWriteContext.index_ == (2 * k_)) {
			while (BaseBufferPatterns_[myLocation + 1].get()) {
			} // TODO: possible bottleneck
		}

		int index = (2 * k_ * myLocation) + threadWriteContext.index_;
		BaseBuffers_[index] = dataItem;
		threadWriteContext.index_++;

		if (threadWriteContext.index_ == (2 * k_)) {
			Arrays.sort(BaseBuffers_, (2 * k_ * myLocation), (2 * k_ * (myLocation + 1)));
			int startIndex = 2 * k_ * myLocation;
			FlexDoublesArrayAccessor bufIn = FlexDoublesArrayAccessor.wrap(BaseBuffers_, startIndex, 2 * k_);
			FlexDoublesArrayAccessor bufOut = FlexDoublesArrayAccessor.wrap(BaseBuffers_, startIndex, k_);
			DoublesUpdateImpl.zipSize2KBuffer(bufIn, bufOut);
			BaseBufferPatterns_[myLocation].set(true);
		}

		if (threadWriteContext.index_ == (2 * 2 * k_)) {
			Arrays.sort(BaseBuffers_, (2 * k_ * (myLocation + 1)), (2 * k_ * (myLocation + 2)));
			int startIndex = 2 * k_ * (myLocation + 1);
			FlexDoublesArrayAccessor bufIn = FlexDoublesArrayAccessor.wrap(BaseBuffers_, startIndex, 2 * k_);
			FlexDoublesArrayAccessor bufOut = FlexDoublesArrayAccessor.wrap(BaseBuffers_, startIndex, k_);
			DoublesUpdateImpl.zipSize2KBuffer(bufIn, bufOut);

			BaseBufferPatterns_[myLocation + 1].set(true);
			threadWriteContext.index_ = 0;
		}

		// if (threadWriteContext.index_ == (2 * k_)) {
		//
		// int baseTreeNodeNum = getFreeTreeBasePlace(numberOfTreeLevels_,
		// threadWriteContext);
		//// assert (treeBaseBitPattern_[baseTreeNodeNum - 1].get() == 0);
		//
		// // LOG.info("baseTreeNodeNum = " + baseTreeNodeNum);
		//
		// int dstInd = (baseTreeNodeNum - 1) * 2 * k_;
		// System.arraycopy(threadWriteContext.buffer_, 0, TreeBaseBuffer_.getBuffer_(),
		// dstInd, 2 * k_);
		// treeBaseBitPattern_[baseTreeNodeNum - 1].set(1);
		// treeBaselock_[baseTreeNodeNum - 1].set(false);
		//
		// assert (baseTreeNodeNum > 1);
		//
		// BackgroundBaseTreeWriter job = new BackgroundBaseTreeWriter("SORT_ZIP",
		// baseTreeNodeNum, this);
		// executorService_.execute(job);
		//
		// // numberOfLeaves =
		// // MWMRHeapUpdateDoublesSketch.numberOfLeaves(numberOfTreeLevels_);
		//
		// threadWriteContext.index_ = 0;
		// }

	}

	// TODO: check the logic.
	@Override
	public double getQuantile(final double fraction) {

		// double[] a = new double[1000];
		//
		// for (int i = 0; i < 1000 ; i++) {
		// a[i] = i;
		// }

		// return 0;

		// long endTime = System.currentTimeMillis() + 1000;
		// while (true) {
		// long left = endTime - System.currentTimeMillis();
		// if (left <= 0)
		// break;
		// }
		//
		// return 0;

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

//	int getFreeTreeBasePlace(int numberOfTreeLevels, ThreadWriteContext threadWrtieContext) {
//
//		int numberOfLeaves = MWMRHeapUpdateDoublesSketch.numberOfLeaves(numberOfTreeLevels);
//		int curr = threadWrtieContext.NextBaseTreeNodeNum_;
//
//		// LOG.info("before while. curr = " + curr);
//
//		int startLeaf;
//		int endLeaf;
//
//		if (threadWrtieContext.id_ == 1) {
//			startLeaf = 1;
//			endLeaf = numberOfLeaves;
//
//		} else {
//			startLeaf = 1 + (threadWrtieContext.id_ - 1 - 1) * (numberOfLeaves / numberOfWriters_);
//			endLeaf = (threadWrtieContext.id_ - 1) * (numberOfLeaves / numberOfWriters_);
//		}
//
//		while (true) {
//
//			int next = curr + 1;
//			if (threadWrtieContext.steal_ == false) {
//
//				if (next > endLeaf) {
//					next = startLeaf;
//				}
//
//				if (next == threadWrtieContext.NextBaseTreeNodeNum_) {
//					threadWrtieContext.steal_ = true;
//					next = endLeaf + 1;
//					if (next > numberOfLeaves) {
//						next = 1;
//					}
//				}
//			} else {
//				if (next > numberOfLeaves) {
//					next = 1;
//				}
//			}
//
//			// LOG.info("in while: curr = " + curr);
//			if (treeBaseBitPattern_[curr - 1].get() == 0) {
//				// TODO: add lock here..
//				if (treeBaselock_[curr - 1].compareAndSet(false, true)) {
//					if (treeBaseBitPattern_[curr - 1].get() == 0) {
//
//						if (threadWrtieContext.steal_ = true) {
//							threadWrtieContext.NextBaseTreeNodeNum_ = startLeaf;
//						} else {
//							threadWrtieContext.NextBaseTreeNodeNum_ = next;
//						}
//						threadWrtieContext.steal_ = false;
//						return curr;
//					} else {
//						treeBaselock_[curr - 1].set(false);
//					}
//				}
//			}
//
//			// else {
//			// debug_++;
//			// }
//
//			curr = next;
//		}
//	}

	public void resetLocal() {

		threadReadLocal_.set(null);
		threadWriteLocal_.set(null);

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

		// for (int ind = index; ind > 0; ind >> 1) {
		// if ( (bits & ind) > 0 ) {
		// n += (k_ * weight);
		// }
		// weight = weight / 2;
		// }

		return n;
	}

//	private long power(int base, int arg) {
//		assert (arg >= 0);
//
//		long res;
//
//		if (arg == 0) {
//			return 1;
//		} else {
//			res = base;
//			arg--;
//		}
//
//		while (arg > 0) {
//			res *= base;
//			arg--;
//		}
//
//		return res;
//	}

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

//	private void collectOnce(HeapUpdateDoublesSketch ds, int startingLevel, int endingLevel) {
//
//		int startIndex;
//		int endIndex;
//
//		if (startingLevel == -1) {
//			startIndex = 0;
//		} else {
//			startIndex = (2 * k_) + (startingLevel * k_);
//		}
//
//		endIndex = (2 * k_) + (endingLevel * k_);
//
//		int indexNum = endIndex - startIndex;
//
//		System.arraycopy(getCombinedBuffer(), startIndex, ds.getCombinedBuffer(), startIndex, indexNum);
//
//	}

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

//	@Override
//	public void reset() {
//
//		final int baseBufAlloc = 2 * k_;
//
//		this.executorService_.shutdown();
//		this.putN(0);
//		this.putCombinedBuffer(new double[baseBufAlloc]);
//		this.putBaseBufferCount(0);
//		this.putBitPattern(0); // represent also the base buffer.
//		this.putMinValue(Double.POSITIVE_INFINITY);
//		this.putMaxValue(Double.NEGATIVE_INFINITY);
//		this.executorService_ = Executors.newFixedThreadPool(numberOfThreads_);
//
//		int numberOfLeaves = numberOfLeaves(numberOfTreeLevels_);
//
//		for (int i = 0; i < numberOfLeaves; i++) {
//			treeBaseBitPattern_[i].set(0);
//		}
//
//		int numberOfTreeNodes = numberOfTreeNodes(numberOfTreeLevels_);
//
//		for (int i = 0; i < numberOfTreeNodes; i++) {
//			treeBitPattern_[i].set(0);
//		}
//
//		for (int i = 0; i < numberOfTreeNodes; i++) {
//			treeReadyBitPattern_[i].set(0);
//		}
//
//	}

	public void clean() {

		Propogator_.setRun_(false);
		
		try {
			Propogator_.join();
		}catch (Exception e) {
			LOG.info("exception: " + e);
		}
		

		// TODO: kill the background thread!
		// Propogator_.stop();

		// try {
		// executorService_.shutdown();
		// executorService_.awaitTermination(5, TimeUnit.SECONDS); // blocks/waits for
		// certain interval as specified
		// executorService_.shutdownNow();
		// } catch (Exception e) {
		// LOG.info("Exception: " + e);
		// }
	}

	// @Override
	// void putBaseBufferCount(final int baseBufferCount) {
	// atomicBaseBufferCount_.set(baseBufferCount);
	// }

	@Override
	void putBitPattern(final long bitPattern) {
		atomicBitPattern_.set(bitPattern);
	}

	// @Override
	// int getBaseBufferCount() {
	// return atomicBaseBufferCount_.get();
	// }

	@Override
	long getBitPattern() {
		return atomicBitPattern_.get();
	}

//	public DoublesArrayAccessor getTreeBaseBuffer_() {
//		return TreeBaseBuffer_;
//	}

//	public AtomicInteger[] getTreeBaseBitPattern_() {
//		return treeBaseBitPattern_;
//	}

//	public DoublesArrayAccessor getTreeBuffer_() {
//		return treeBuffer_;
//	}

//	public AtomicInteger[] getTreeBitPattern_() {
//		return treeBitPattern_;
//	}

//	public int getNumberOfTreeLevels_() {
//		return numberOfTreeLevels_;
//	}

//	public ExecutorService getExecutorSevice_() {
//		return executorService_;
//	}
//
//	public AtomicInteger[] getTreeReadyBitPattern_() {
//		return treeReadyBitPattern_;
//	}
//
//	public AtomicInteger getPropogationLock() {
//		return PropogationLock_;
//	}

//	public void setPropogationLock(AtomicInteger propogationLock_) {
//		PropogationLock_ = propogationLock_;
//	}

//	private abstract class BackgroundWriter implements Runnable {
//
//		private ExecutionType type_;
//		private MWMRHeapUpdateDoublesSketch ds_;
//
//		public BackgroundWriter(String type, MWMRHeapUpdateDoublesSketch ds) {
//			type_ = ExecutionType.valueOf(type);
//			ds_ = ds;
//		}
//
//		@Override
//		public abstract void run();
//
//		public void setType(String type) {
//			type_ = ExecutionType.valueOf(type);
//		}
//
//		public void setSketch(MWMRHeapUpdateDoublesSketch ds) {
//			ds_ = ds;
//		}
//
//		public ExecutionType getType() {
//			return type_;
//		}
//
//		public MWMRHeapUpdateDoublesSketch getSketch() {
//			return ds_;
//		}
//
//		public void makeAndRunNewJob(int loocationInTree, MWMRHeapUpdateDoublesSketch ds) {
//
//			if (loocationInTree == 1) { // you are at the root
//				// assert (ds.getNumberOfTreeLevels_() == 1);
//
//				BackgroundPropogatorWriter job = new BackgroundPropogatorWriter("PROPOGATOR", ds);
//				job.run(); // Is this the right way to do it?
//
//			} else {
//
//				int fatherIndex = (int) (loocationInTree / 2);
//				treeReadyBitPattern_[fatherIndex - 1]++;
//				int numberOfChildrenReady = treeReadyBitPattern_[fatherIndex - 1];
//				assert (numberOfChildrenReady < 3 && numberOfChildrenReady > 0);
//				if (numberOfChildrenReady == 2) {
//
//					BackgroundTreeWriter job = new BackgroundTreeWriter("MARGESORT_ZIP", fatherIndex, ds);
//					job.run();
//
//				} else {
//					return;
//				}
//
//			}
//		}
//
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
//
//	}

	public  class BackgroundPropogator extends Thread {

		private volatile boolean run_ = true;
		private int numberOfThreads_;

		BackgroundPropogator(int numberOfThreads) {
			numberOfThreads_ = numberOfThreads;
		}

		@Override
		public void run() {

			int startFrom = 0;
			int locationToPropogate;

			while (true) {

				locationToPropogate = findWork(startFrom);
				if (locationToPropogate == -1) {
					return;
				} else {
					startFrom = ((locationToPropogate + 1) % numberOfThreads_);
				}

				propogate(locationToPropogate);

			}

		}

		int findWork(int startFrom) {

			int i = 0;
			int index = startFrom;

			while (true) {

				if (i == 10000000) { // fence only when you really think the test is over.
					if (!run_) {
						return -1;
					} else {
						i = 0;
					}
				}

				if (BaseBufferPatterns_[index].get()) {
					return index;
				} else {
					index = (index + 1) % numberOfThreads_;
					i++;
					// TODO if index == startFrom then sleep for a while.
				}
			}
		}

		void propogate(int locationToPropogate) {

			//
			long bitPattern = getBitPattern();
			int startingLevel = 0;

			long newN = getN() + (2 * k_);

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

			if (endingLevel == 0) {
				tgtSketchBuf.putArray(BaseBuffers_, locationToPropogate * 2 * k_, 0, k_);
				BaseBufferPatterns_[locationToPropogate].set(false);

			} else {
				DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(MWMRHeapUpdateDoublesSketch.this, true);
				FlexDoublesArrayAccessor basesAccesor = FlexDoublesArrayAccessor.wrap(BaseBuffers_,
						locationToPropogate * 2 * k_, k_);
				DoublesSketchAccessor firstLevelBuf = tgtSketchBuf.copyAndSetLevel(0);
				DoublesUpdateImpl.mergeTwoSizeKBuffers(firstLevelBuf, // target level: lvl
						basesAccesor, // target level: endingLevel
						bbAccessor);
				BaseBufferPatterns_[locationToPropogate].set(false);
				DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, tgtSketchBuf);

				for (int lvl = 1; lvl < endingLevel; lvl++) {
					assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
					final DoublesSketchAccessor currLevelBuf = tgtSketchBuf.copyAndSetLevel(lvl);
					DoublesUpdateImpl.mergeTwoSizeKBuffers(currLevelBuf, // target level: lvl
							tgtSketchBuf, // target level: endingLevel
							bbAccessor);
					DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, tgtSketchBuf);
				} // end of loop over lower levels
			}

			// tgtSketchBuf.putArray(treeBuffer_.getBuffer_(), 0, 0, ds.k_);
			//
			// treeBitPattern_[0] = 0;
			// // TODO propagate
			//
			// final DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(ds,
			// true);
			//
			// for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
			// assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
			// final DoublesSketchAccessor currLevelBuf = tgtSketchBuf.copyAndSetLevel(lvl);
			// DoublesUpdateImpl.mergeTwoSizeKBuffers(currLevelBuf, // target level: lvl
			// tgtSketchBuf, // target level: endingLevel
			// bbAccessor);
			// DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, tgtSketchBuf);
			// } // end of loop over lower levels

			// update bit pattern with binary-arithmetic ripple carry
			long newBitPattern = bitPattern + (1L << startingLevel);
			assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check
			// assert newBitPattern == ds.getBitPattern() + 1 || newBitPattern = (1L <<
			// startingLevel);

			putBitPattern(newBitPattern);
			// baseBufferCount_ = 0;
			putN(newN);

		}

		public void setRun_(boolean run_) {
			this.run_ = run_;
		}

	}

//	public class BackgroundPropogatorWriter extends BackgroundWriter {
//
//		// TODO check if no one else propogates.
//
//		public BackgroundPropogatorWriter(String type, MWMRHeapUpdateDoublesSketch ds) {
//			super(type, ds);
//		}
//
//		@Override
//		public void run() {
//
//			// setAffinity();
//
//			// MWMRHeapUpdateDoublesSketch ds = getSketch();
//			// DoublesArrayAccessor TreeBuffer = ds.getTreeBuffer_();
//			// AtomicInteger[] TreeBitPattern = ds.getTreeBitPattern_();
//			// AtomicInteger PropogationLock = ds.getPropogationLock();
//
//			assert (treeBitPattern_[0] == 1);
//			// if (PropogationLock.get() == 1) {
//			// ds.getExecutorSevice_().execute(this);
//			// // LOG.info("Propogation is locked");
//			// return;
//			// }
//
//			// long threadId = Thread.currentThread().getId();
//			// LOG.info("Thread # " + threadId + " is doing this task");
//
//			// PropogationLock.set(1);
//
//			// copy to empty place in combained buffer
//			long bitPattern = getBitPattern();
//			int startingLevel = getNumberOfTreeLevels_() - 1;
//
//			long newN = getN() + MWMRHeapUpdateDoublesSketch.numberOfLeaves(startingLevel + 1) * 2 * ds.k_;
//
//			// make sure there will be enough space (levels) for the propagation
//			final int spaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(k_, newN);
//			final int combBufItemCap = getCombinedBufferItemCapacity();
//
//			if (spaceNeeded > combBufItemCap) {
//				// copies base buffer plus old levels, adds space for new level
//				growCombinedBuffer(combBufItemCap, spaceNeeded);
//			}
//
//			DoublesSketchAccessor tgtSketchBuf = DoublesSketchAccessor.wrap(ds, true);
//			final int endingLevel = Util.lowestZeroBitStartingAt(bitPattern, startingLevel);
//			tgtSketchBuf.setLevel(endingLevel);
//			tgtSketchBuf.putArray(treeBuffer_.getBuffer_(), 0, 0, ds.k_);
//
//			treeBitPattern_[0] = 0;
//			// TODO propagate
//
//			final DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(ds, true);
//
//			for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
//				assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
//				final DoublesSketchAccessor currLevelBuf = tgtSketchBuf.copyAndSetLevel(lvl);
//				DoublesUpdateImpl.mergeTwoSizeKBuffers(currLevelBuf, // target level: lvl
//						tgtSketchBuf, // target level: endingLevel
//						bbAccessor);
//				DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, tgtSketchBuf);
//			} // end of loop over lower levels
//
//			// update bit pattern with binary-arithmetic ripple carry
//			long newBitPattern = bitPattern + (1L << startingLevel);
//			assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check
//			// assert newBitPattern == ds.getBitPattern() + 1 || newBitPattern = (1L <<
//			// startingLevel);
//
//			putBitPattern(newBitPattern);
//			// baseBufferCount_ = 0;
//			putN(newN);
//			// PropogationLock.set(0);
//
//		}
//
//	}

//	public class BackgroundBaseTreeWriter extends BackgroundWriter { // Important: only one writer can generate this job
//																		// per base node.
//
//		private int nodeNumber_;
//
//		public BackgroundBaseTreeWriter(String type, int nodesNumber, MWMRHeapUpdateDoublesSketch ds) {
//			super(type, ds);
//			nodeNumber_ = nodesNumber;
//		}
//
//		@Override
//		public void run() {
//
//			// setAffinity();
//
//			// MWMRHeapUpdateDoublesSketch ds = getSketch();
//			// DoublesArrayAccessor TreeBaseBuffer = ds.getTreeBaseBuffer_();
//			// AtomicInteger[] TreeBaseBitPattern = ds.getTreeBaseBitPattern_();
//			// DoublesArrayAccessor TreeBuffer = ds.getTreeBuffer_();
//			// AtomicInteger[] TreeBitPattern = ds.getTreeBitPattern_();
//			// AtomicInteger[] TreeReadyBitPattern = ds.getTreeReadyBitPattern_();
//
//			// assert (treeBaseBitPattern_[nodeNumber_ - 1].get() == 1);
//			int NumberOfTreeInerNodes = (int) Math.pow(2, getNumberOfTreeLevels_() - 1) - 1;
//			int leaveIndex = NumberOfTreeInerNodes + nodeNumber_;
//
//			if (treeBitPattern_[leaveIndex - 1] == 1) {
//				executorService_.execute(this);
//				// LOG.info("BackgroundBaseTreeWriter: node num = " + nodeNumber_);
//				return;
//			} else {
//				Arrays.sort(TreeBaseBuffer.getBuffer_(), (nodeNumber_ - 1) * 2 * k_, nodeNumber_ * 2 * k_);
//				FlexDoublesArrayAccessor srcBuffer = FlexDoublesArrayAccessor.wrap(TreeBaseBuffer.getBuffer_(),
//						(nodeNumber_ - 1) * 2 * k_, 2 * k_);
//				FlexDoublesArrayAccessor dstBufer = FlexDoublesArrayAccessor.wrap(treeBuffer_.getBuffer_(),
//						(leaveIndex - 1) * k_, k_);
//				DoublesUpdateImpl.zipSize2KBuffer(srcBuffer, dstBufer);
//				treeBitPattern_[leaveIndex - 1] = 1;
//				treeBaseBitPattern_[nodeNumber_ - 1].set(0);
//
//				makeAndRunNewJob(leaveIndex, ds);
//
//			}
//		}
//
//	}

//	public class BackgroundTreeWriter extends BackgroundWriter {
//
//		private int nodeNumber_;
//
//		public BackgroundTreeWriter(String type, int nodesNumber, MWMRHeapUpdateDoublesSketch ds) {
//			super(type, ds);
//			nodeNumber_ = nodesNumber;
//		}
//
//		@Override
//		public void run() {
//
//			// setAffinity();
//
//			// MWMRHeapUpdateDoublesSketch ds = getSketch();
//			// DoublesArrayAccessor TreeBuffer = ds.getTreeBuffer_();
//			// AtomicInteger[] TreeBitPattern = ds.getTreeBitPattern_();
//			// AtomicInteger[] TreeReadyBitPattern = ds.getTreeReadyBitPattern_();
//
//			assert (treeReadyBitPattern_[nodeNumber_ - 1] == 2);
//			if (treeBitPattern_[nodeNumber_ - 1] == 1) {
//				getExecutorSevice_().execute(this);
//				return;
//			}
//
//			ThreadHelperContext THC = threadHelperContext_.get();
//			if (THC == null) {
//				THC = new ThreadHelperContext();
//				THC.buffer2k_ = DoublesArrayAccessor.initialize(2 * k_);
//				threadHelperContext_.set(THC);
//			}
//
//			// lock the location
//			treeBitPattern_[nodeNumber_ - 1] = 1;
//			// reset ready for next iteration
//			treeReadyBitPattern_[nodeNumber_ - 1] = 0;
//
//			// copy children to local memory and unlock their locations.
//			int leftChildInd = (2 * nodeNumber_);
//			assert (treeBitPattern_[leftChildInd - 1] == 1);
//			int fromLeftChild = (leftChildInd - 1) * k_;
//			// double[] leftChild = TreeBuffer.getArray(fromInd, ds.k_);
//			// TreeBitPattern[leftChildInd - 1].set(0);
//
//			int rightChildInd = (2 * nodeNumber_) + 1;
//			assert (treeBitPattern_[rightChildInd - 1] == 1);
//			int fromRightChild = (rightChildInd - 1) * k_;
//			// double[] rightChild = TreeBuffer.getArray(fromInd, ds.k_);
//			// TreeBitPattern[rightChildInd - 1].set(0);
//
//			// merge sort and zip into the node.
//
//			// FlexDoublesArrayAccessor leftSource =
//			// FlexDoublesArrayAccessor.wrap(leftChild, 0, ds.k_);
//			// FlexDoublesArrayAccessor rightSource =
//			// FlexDoublesArrayAccessor.wrap(rightChild, 0, ds.k_);
//			FlexDoublesArrayAccessor leftSource = FlexDoublesArrayAccessor.wrap(treeBuffer_.getBuffer_(), fromLeftChild,
//					k_);
//			FlexDoublesArrayAccessor rightSource = FlexDoublesArrayAccessor.wrap(treeBuffer_.getBuffer_(),
//					fromRightChild, k_);
//			// DoublesArrayAccessor tmp = THC.buffer2k_;
//
//			DoublesUpdateImpl.mergeTwoSizeKBuffers(leftSource, rightSource, THC.buffer2k_); //
//
//			treeBitPattern_[leftChildInd - 1] = 0;
//			treeBitPattern_[rightChildInd - 1] = 0;
//
//			FlexDoublesArrayAccessor dst = FlexDoublesArrayAccessor.wrap(treeBuffer_.getBuffer_(),
//					(nodeNumber_ - 1) * k_, k_);
//			DoublesUpdateImpl.zipSize2KBuffer(THC.buffer2k_, dst);
//
//			makeAndRunNewJob(nodeNumber_, ds);
//		}
//
//	}

}
