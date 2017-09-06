package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

	private static class ThreadWriteContext {
		double[] buffer_;
		int index_;
		int NextBaseTreeNodeNum_;
		int id_;
	}

	enum ExecutionType {
		PROPOGATOR, SORT_ZIP, MARGESORT_ZIP
	}

	/**
	 * 
	 */

	private final ThreadLocal<ThreadReadContext> threadReadLocal_ = new ThreadLocal<ThreadReadContext>();
	private final ThreadLocal<ThreadWriteContext> threadWriteLocal_ = new ThreadLocal<ThreadWriteContext>();
	public final ThreadLocal<ThreadAffinityContext> threadAffinityLocal_ = new ThreadLocal<ThreadAffinityContext>();
	private AtomicLong atomicBitPattern_ = new AtomicLong();
	private AtomicInteger atomicBaseBufferCount_ = new AtomicInteger();
	private ExecutorService executorSevice_;
	private int numberOfThreads_;
	private int numberOfTreeLevels_;
	private int numberOfWriters_;
	private DoublesArrayAccessor TreeBaseBuffer_;
	private AtomicInteger[] treeBaseBitPattern_;
	private DoublesArrayAccessor treeBuffer_;
	private AtomicInteger[] treeBitPattern_;
	private AtomicInteger[] treeReadyBitPattern_;
	private AtomicInteger PropogationLock_;

	private AtomicInteger WritersID = new AtomicInteger();
	public AtomicInteger Affinity_ = new AtomicInteger();

	public int debug_ = 0;

	@Override
	public int getDebug_() {
		return debug_;
	}

	private static final Log LOG = LogFactory.getLog(MWMRHeapUpdateDoublesSketch.class);

	// **CONSTRUCTORS**********************************************************
	public MWMRHeapUpdateDoublesSketch(final int k) {
		super(k); // Checks k
	}

	static MWMRHeapUpdateDoublesSketch newInstance(final int k, int numberOfThreads, int numberOfTreeLevels,
			int numberOfWriters) {

		final MWMRHeapUpdateDoublesSketch hqs = new MWMRHeapUpdateDoublesSketch(k);
		final int baseBufAlloc = 2 * k; // the min is important

		hqs.putN(0);
		hqs.putCombinedBuffer(new double[baseBufAlloc]);
		hqs.putBaseBufferCount(0);
		hqs.putBitPattern(0); // represent also the base buffer.
		hqs.putMinValue(Double.POSITIVE_INFINITY);
		hqs.putMaxValue(Double.NEGATIVE_INFINITY);
		hqs.numberOfThreads_ = numberOfThreads;
		hqs.numberOfTreeLevels_ = numberOfTreeLevels;
		hqs.numberOfWriters_ = numberOfWriters;
		hqs.executorSevice_ = Executors.newFixedThreadPool(numberOfThreads);

		int numberOfLeaves = numberOfLeaves(numberOfTreeLevels);
		hqs.TreeBaseBuffer_ = DoublesArrayAccessor.initialize(numberOfLeaves * 2 * k);

		hqs.treeBaseBitPattern_ = new AtomicInteger[numberOfLeaves];
		for (int i = 0; i < numberOfLeaves; i++) {
			hqs.treeBaseBitPattern_[i] = new AtomicInteger();
		}

		int numberOfTreeNodes = numberOfTreeNodes(numberOfTreeLevels);
		hqs.treeBuffer_ = DoublesArrayAccessor.initialize(numberOfTreeNodes * k);

		hqs.treeBitPattern_ = new AtomicInteger[numberOfTreeNodes];
		for (int i = 0; i < numberOfTreeNodes; i++) {
			hqs.treeBitPattern_[i] = new AtomicInteger();
		}

		hqs.treeReadyBitPattern_ = new AtomicInteger[numberOfTreeNodes];
		for (int i = 0; i < numberOfTreeNodes; i++) {
			hqs.treeReadyBitPattern_[i] = new AtomicInteger();
		}

		hqs.PropogationLock_ = new AtomicInteger();

		return hqs;
	}

	static int numberOfLeaves(int numberOfTreeLevels) {
		return (int) Math.pow(2, (numberOfTreeLevels - 1));
	}

	static int numberOfTreeNodes(int numberOfTreeLevels) {
		return (int) (Math.pow(2, numberOfTreeLevels) - 1);
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

//		try {
//			ThreadAffinityContext threadAffinityContext = threadAffinityLocal_.get();
//			if (threadAffinityContext == null) {
//				threadAffinityContext = new ThreadAffinityContext();
//				threadAffinityContext.affinitiyIsSet = true;
//
//				int core = Affinity_.getAndIncrement();
//
//				LOG.info("I am a writer and my core is " + ThreadAffinity.currentCore());
//				long mask = 1 << core;
//				ThreadAffinity.setCurrentThreadAffinityMask(mask);
//				LOG.info("I am a writer and my core is " + ThreadAffinity.currentCore());
//			}
//		} catch (Exception e) {
//			// TODO: handle exception
//			LOG.info("catched RuntimeException: " + e);
//		}

		ThreadWriteContext threadWriteContext = threadWriteLocal_.get();
		if (threadWriteContext == null) {
			threadWriteContext = new ThreadWriteContext();

			threadWriteContext.index_ = 0;
			threadWriteContext.buffer_ = new double[2 * k_];
			threadWriteContext.id_ = WritersID.incrementAndGet();

			if (threadWriteContext.id_ == 1) {
				threadWriteContext.NextBaseTreeNodeNum_ = 1;
			} else {
				threadWriteContext.NextBaseTreeNodeNum_ = threadWriteContext.id_ - 1;
			}

			threadWriteLocal_.set(threadWriteContext);
		}

		threadWriteContext.buffer_[threadWriteContext.index_] = dataItem;
		threadWriteContext.index_++;

		if (threadWriteContext.index_ == (2 * k_)) {

			int baseTreeNodeNum = getFreeTreeBasePlace(numberOfTreeLevels_, threadWriteContext);
			assert (treeBaseBitPattern_[baseTreeNodeNum - 1].get() == 0);

			// LOG.info("baseTreeNodeNum = " + baseTreeNodeNum);

			int dstInd = (baseTreeNodeNum - 1) * 2 * k_;
			System.arraycopy(threadWriteContext.buffer_, 0, TreeBaseBuffer_.getBuffer_(), dstInd, 2 * k_);
			treeBaseBitPattern_[baseTreeNodeNum - 1].set(1);

			BackgroundBaseTreeWriter job = new BackgroundBaseTreeWriter("SORT_ZIP", baseTreeNodeNum, this);
			executorSevice_.execute(job);

			// numberOfLeaves =
			// MWMRHeapUpdateDoublesSketch.numberOfLeaves(numberOfTreeLevels_);

			threadWriteContext.index_ = 0;
		}

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

	int getFreeTreeBasePlace(int numberOfTreeLevels, ThreadWriteContext threadWrtieContext) {

		int numberOfLeaves = MWMRHeapUpdateDoublesSketch.numberOfLeaves(numberOfTreeLevels);
		int curr = threadWrtieContext.NextBaseTreeNodeNum_;

		// LOG.info("before while. curr = " + curr);

		int skip;
		int id;

		if (threadWrtieContext.id_ == 1) {
			skip = 1;
			id = 1;
		} else {
			id = threadWrtieContext.id_ - 1;
			skip = numberOfWriters_;
		}

		while (true) {

			// int next = curr + numberOfWriters_;
			// if (next > numberOfLeaves) {
			// next = threadWrtieContext.id_;
			// }

			int next = curr + skip;
			if (next > numberOfLeaves) {
				next = id;
			}

			// LOG.info("in while 1: curr = " + curr);
			if (treeBaseBitPattern_[curr - 1].get() == 0) {
				threadWrtieContext.NextBaseTreeNodeNum_ = next;
				return curr;
			}
			// else {
			// debug_++;
			// }
			curr = next;

			// LOG.info("in while 1: curr = " + curr);
		}
	}

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

		this.executorSevice_.shutdown();
		this.putN(0);
		this.putCombinedBuffer(new double[baseBufAlloc]);
		this.putBaseBufferCount(0);
		this.putBitPattern(0); // represent also the base buffer.
		this.putMinValue(Double.POSITIVE_INFINITY);
		this.putMaxValue(Double.NEGATIVE_INFINITY);
		this.executorSevice_ = Executors.newFixedThreadPool(numberOfThreads_);

		int numberOfLeaves = numberOfLeaves(numberOfTreeLevels_);

		for (int i = 0; i < numberOfLeaves; i++) {
			treeBaseBitPattern_[i].set(0);
		}

		int numberOfTreeNodes = numberOfTreeNodes(numberOfTreeLevels_);

		for (int i = 0; i < numberOfTreeNodes; i++) {
			treeBitPattern_[i].set(0);
		}

		for (int i = 0; i < numberOfTreeNodes; i++) {
			treeReadyBitPattern_[i].set(0);
		}

	}

	public void clean() {
		this.executorSevice_.shutdown();
	}

	@Override
	void putBaseBufferCount(final int baseBufferCount) {
		atomicBaseBufferCount_.set(baseBufferCount);
	}

	@Override
	void putBitPattern(final long bitPattern) {
		atomicBitPattern_.set(bitPattern);
	}

	@Override
	int getBaseBufferCount() {
		return atomicBaseBufferCount_.get();
	}

	@Override
	long getBitPattern() {
		return atomicBitPattern_.get();
	}

	public DoublesArrayAccessor getTreeBaseBuffer_() {
		return TreeBaseBuffer_;
	}

	public AtomicInteger[] getTreeBaseBitPattern_() {
		return treeBaseBitPattern_;
	}

	public DoublesArrayAccessor getTreeBuffer_() {
		return treeBuffer_;
	}

	public AtomicInteger[] getTreeBitPattern_() {
		return treeBitPattern_;
	}

	public int getNumberOfTreeLevels_() {
		return numberOfTreeLevels_;
	}

	public ExecutorService getExecutorSevice_() {
		return executorSevice_;
	}

	public AtomicInteger[] getTreeReadyBitPattern_() {
		return treeReadyBitPattern_;
	}

	public AtomicInteger getPropogationLock() {
		return PropogationLock_;
	}

	public void setPropogationLock(AtomicInteger propogationLock_) {
		PropogationLock_ = propogationLock_;
	}

	private abstract class BackgroundWriter implements Runnable {

		private ExecutionType type_;
		private MWMRHeapUpdateDoublesSketch ds_;

		public BackgroundWriter(String type, MWMRHeapUpdateDoublesSketch ds) {
			type_ = ExecutionType.valueOf(type);
			ds_ = ds;
		}

		@Override
		public abstract void run();

		public void setType(String type) {
			type_ = ExecutionType.valueOf(type);
		}

		public void setSketch(MWMRHeapUpdateDoublesSketch ds) {
			ds_ = ds;
		}

		public ExecutionType getType() {
			return type_;
		}

		public MWMRHeapUpdateDoublesSketch getSketch() {
			return ds_;
		}

		public void makeAndRunNewJob(int loocationInTree, MWMRHeapUpdateDoublesSketch ds,
				AtomicInteger[] TreeReadyBitPattern) {

			if (loocationInTree == 1) { // you are at the root
				// assert (ds.getNumberOfTreeLevels_() == 1);

				BackgroundPropogatorWriter job = new BackgroundPropogatorWriter("PROPOGATOR", ds);
				job.run(); // Is this the right way to do it?

			} else {

				int fatherIndex = (int) (loocationInTree / 2);
				int numberOfChildrenReady = TreeReadyBitPattern[fatherIndex - 1].incrementAndGet();
				assert (numberOfChildrenReady < 3 && numberOfChildrenReady > 0);
				if (numberOfChildrenReady == 2) {

					BackgroundTreeWriter job = new BackgroundTreeWriter("MARGESORT_ZIP", fatherIndex, ds);
					job.run();

				} else {
					return;
				}

			}
		}

		public void setAffinity() {

//			try {
//
//				ThreadAffinityContext threadAffinityContext = ds_.threadAffinityLocal_.get();
//				if (threadAffinityContext == null) {
//					threadAffinityContext = new ThreadAffinityContext();
//					threadAffinityContext.affinitiyIsSet = true;
//
//					int core = ds_.Affinity_.getAndIncrement();
//
//					LOG.info("I am a writer and my core is " + ThreadAffinity.currentCore());
//					long mask = 1 << core;
//					ThreadAffinity.setCurrentThreadAffinityMask(mask);
//					LOG.info("I am a writer and my core is " + ThreadAffinity.currentCore());
//				}
//			} catch (Exception e) {
//				// TODO: handle exception
//				LOG.info("catched RuntimeException: " + e);
//			}

		}

	}

	public class BackgroundPropogatorWriter extends BackgroundWriter {

		// TODO check if no one else propogates.

		public BackgroundPropogatorWriter(String type, MWMRHeapUpdateDoublesSketch ds) {
			super(type, ds);
		}

		@Override
		public void run() {

			setAffinity();

			MWMRHeapUpdateDoublesSketch ds = getSketch();
			DoublesArrayAccessor TreeBuffer = ds.getTreeBuffer_();
			AtomicInteger[] TreeBitPattern = ds.getTreeBitPattern_();
			AtomicInteger PropogationLock = ds.getPropogationLock();

			assert (TreeBitPattern[0].get() == 1);
			if (PropogationLock.get() == 1) {
				ds.getExecutorSevice_().execute(this);
				return;
			}

			// long threadId = Thread.currentThread().getId();
			// LOG.info("Thread # " + threadId + " is doing this task");

			PropogationLock.set(1);

			// copy to empty place in combained buffer
			long bitPattern = ds.getBitPattern();
			int startingLevel = ds.getNumberOfTreeLevels_() - 1;

			long newN = ds.getN() + MWMRHeapUpdateDoublesSketch.numberOfLeaves(startingLevel + 1) * 2 * ds.k_;

			// make sure there will be enough space (levels) for the propagation
			final int spaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(k_, newN);
			final int combBufItemCap = ds.getCombinedBufferItemCapacity();

			if (spaceNeeded > combBufItemCap) {
				// copies base buffer plus old levels, adds space for new level
				growCombinedBuffer(combBufItemCap, spaceNeeded);
			}

			DoublesSketchAccessor tgtSketchBuf = DoublesSketchAccessor.wrap(ds, true);
			final int endingLevel = Util.lowestZeroBitStartingAt(bitPattern, startingLevel);
			tgtSketchBuf.setLevel(endingLevel);
			tgtSketchBuf.putArray(TreeBuffer.getBuffer_(), 0, 0, ds.k_);

			TreeBitPattern[0].set(0);
			// TODO propagate

			final DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(ds, true);

			for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
				assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
				final DoublesSketchAccessor currLevelBuf = tgtSketchBuf.copyAndSetLevel(lvl);
				DoublesUpdateImpl.mergeTwoSizeKBuffers(currLevelBuf, // target level: lvl
						tgtSketchBuf, // target level: endingLevel
						bbAccessor);
				DoublesUpdateImpl.zipSize2KBuffer(bbAccessor, tgtSketchBuf);
			} // end of loop over lower levels

			// update bit pattern with binary-arithmetic ripple carry
			long newBitPattern = bitPattern + (1L << startingLevel);
			assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check
			// assert newBitPattern == ds.getBitPattern() + 1 || newBitPattern = (1L <<
			// startingLevel);

			ds.putBitPattern(newBitPattern);
			// baseBufferCount_ = 0;
			ds.putN(newN);
			PropogationLock.set(0);

		}

	}

	public class BackgroundBaseTreeWriter extends BackgroundWriter { // Important: only one writer can generate this job
																		// per base node.

		private int nodeNumber_;

		public BackgroundBaseTreeWriter(String type, int nodesNumber, MWMRHeapUpdateDoublesSketch ds) {
			super(type, ds);
			nodeNumber_ = nodesNumber;
		}

		@Override
		public void run() {

			setAffinity();

			MWMRHeapUpdateDoublesSketch ds = getSketch();
			DoublesArrayAccessor TreeBaseBuffer = ds.getTreeBaseBuffer_();
			AtomicInteger[] TreeBaseBitPattern = ds.getTreeBaseBitPattern_();
			DoublesArrayAccessor TreeBuffer = ds.getTreeBuffer_();
			AtomicInteger[] TreeBitPattern = ds.getTreeBitPattern_();
			AtomicInteger[] TreeReadyBitPattern = ds.getTreeReadyBitPattern_();

			assert (TreeBaseBitPattern[nodeNumber_ - 1].get() == 1);
			int NumberOfTreeInerNodes = (int) Math.pow(2, ds.getNumberOfTreeLevels_() - 1) - 1;
			int leaveIndex = NumberOfTreeInerNodes + nodeNumber_;

			if (TreeBitPattern[leaveIndex - 1].get() == 1) {
				ds.getExecutorSevice_().execute(this);
				return;
			} else {
				Arrays.sort(TreeBaseBuffer.getBuffer_(), (nodeNumber_ - 1) * 2 * k_, nodeNumber_ * 2 * ds.k_);
				FlexDoublesArrayAccessor srcBuffer = FlexDoublesArrayAccessor.wrap(TreeBaseBuffer.getBuffer_(),
						(nodeNumber_ - 1) * 2 * ds.k_, 2 * ds.k_);
				FlexDoublesArrayAccessor dstBufer = FlexDoublesArrayAccessor.wrap(TreeBuffer.getBuffer_(),
						(leaveIndex - 1) * ds.k_, ds.k_);
				DoublesUpdateImpl.zipSize2KBuffer(srcBuffer, dstBufer);
				TreeBitPattern[leaveIndex - 1].set(1);
				TreeBaseBitPattern[nodeNumber_ - 1].set(0);

				makeAndRunNewJob(leaveIndex, ds, TreeReadyBitPattern);

			}
		}

	}

	public class BackgroundTreeWriter extends BackgroundWriter {

		private int nodeNumber_;

		public BackgroundTreeWriter(String type, int nodesNumber, MWMRHeapUpdateDoublesSketch ds) {
			super(type, ds);
			nodeNumber_ = nodesNumber;
		}

		@Override
		public void run() {

			setAffinity();

			MWMRHeapUpdateDoublesSketch ds = getSketch();
			DoublesArrayAccessor TreeBuffer = ds.getTreeBuffer_();
			AtomicInteger[] TreeBitPattern = ds.getTreeBitPattern_();
			AtomicInteger[] TreeReadyBitPattern = ds.getTreeReadyBitPattern_();

			assert (TreeReadyBitPattern[nodeNumber_ - 1].get() == 2);
			if (TreeBitPattern[nodeNumber_ - 1].get() == 1) {
				ds.getExecutorSevice_().execute(this);
				return;
			}

			// lock the location
			TreeBitPattern[nodeNumber_ - 1].set(1);
			// reset ready for next iteration
			TreeReadyBitPattern[nodeNumber_ - 1].set(0);

			// copy children to local memory and unlock their locations.
			int leftChildInd = (2 * nodeNumber_);
			assert (TreeBitPattern[leftChildInd - 1].get() == 1);
			int fromInd = (leftChildInd - 1) * ds.k_;
			double[] leftChild = TreeBuffer.getArray(fromInd, ds.k_);
			TreeBitPattern[leftChildInd - 1].set(0);

			int rightChildInd = (2 * nodeNumber_) + 1;
			assert (TreeBitPattern[rightChildInd - 1].get() == 1);
			fromInd = (rightChildInd - 1) * ds.k_;
			double[] rightChild = TreeBuffer.getArray(fromInd, ds.k_);
			TreeBitPattern[rightChildInd - 1].set(0);

			// merge sort and zip into the node.

			FlexDoublesArrayAccessor leftSource = FlexDoublesArrayAccessor.wrap(leftChild, 0, ds.k_);
			FlexDoublesArrayAccessor rightSource = FlexDoublesArrayAccessor.wrap(rightChild, 0, ds.k_);
			DoublesArrayAccessor tmp = DoublesArrayAccessor.initialize(2 * ds.k_);

			DoublesUpdateImpl.mergeTwoSizeKBuffers(leftSource, rightSource, tmp);

			FlexDoublesArrayAccessor dst = FlexDoublesArrayAccessor.wrap(TreeBuffer.getBuffer_(),
					(nodeNumber_ - 1) * ds.k_, ds.k_);
			DoublesUpdateImpl.zipSize2KBuffer(tmp, dst);

			makeAndRunNewJob(nodeNumber_, ds, TreeReadyBitPattern);
		}

	}

}
