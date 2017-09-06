package com.yahoo.sketches.quantiles;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

import utils.ConcurrencyTestUtils.TestContext;
import utils.ConcurrencyTestUtils.TestThread;

import com.yahoo.sketches.quantiles.HeapUpdateDoublesSketch;
import com.yahoo.sketches.quantiles.ConccurencyFramworkTest.SketchType;

public class TestPerformance {

	private HeapUpdateDoublesSketch ds_;
	// private static final SketchType type_ = ;
	private final int k_ = 128;
	public final Log LOG = LogFactory.getLog(TestPerformance.class);

	public static void main(String[] args) throws Exception {

		TestPerformance test = new TestPerformance();

		try {
			test.LOG.info("I am the main thread and my core is " + ThreadAffinity.currentCore());
			long mask = 1 << 12;
			ThreadAffinity.setCurrentThreadAffinityMask(mask);
			test.LOG.info("I am the main thread and my core is " + ThreadAffinity.currentCore());
		} catch (Exception e) {
			// TODO: handle exception
			test.LOG.info("catched RuntimeException: " + e);
		}

		// for (int i = 0; i < 3; i++) {
		// test.setUp("ORIGINAL", 1, 3, 1);
		// test.runTest(1, 0, 0, 2);
		// }
		//
		// for (int i = 0; i < 3; i++) {
		// test.setUp("LOCK_BASE_OIGENAL", 1, 3, 1);
		// test.runTest(1, 0, 0, 2);
		// }

		for (int helpers = 1; helpers < 5; helpers++) {
			test.LOG.info("##########################################helpers = " + helpers
					+ "#############################################");
			for (int writers = 1; writers < 5; writers++) {

				test.LOG.info("##########################################writers = " + writers
						+ "#############################################");
				if (writers == 3) {
					continue;
				}

				for (int i = 0; i < 3; i++) {
					test.setUp("MWMR_BASIC", helpers, 3, writers);
					test.runTest(writers, 0, 0, 2);
				}

			}

		}
		//

		test.LOG.info("Done!");

	}

	public void setUp(String t, int numberOfThreads, int numberOfTreeLevels, int numberOfWriters) throws Exception {

		SketchType type_ = SketchType.valueOf(t);

		switch (type_) {
		case ORIGINAL:
			ds_ = HeapUpdateDoublesSketch.newInstance(k_);
			LOG.info(
					"=============================================ORIGINAL=============================================");
			break;
		case SWMR_BASIC:
			ds_ = SWSRHeapUpdateDoublesSketch.newInstance(k_);
			LOG.info(
					"=============================================SWMR_BASIC===========================================");
			break;
		case LOCK_BASE_OIGENAL:
			ds_ = new LockBasedHeapUpdateDoublesSketch(k_);
			LOG.info(
					"=============================================LOCK_BASE_OIGENAL====================================");
			break;
		case MWMR_BASIC:
			ds_ = MWMRHeapUpdateDoublesSketch.newInstance(k_, numberOfThreads, numberOfTreeLevels, numberOfWriters);
			LOG.info(
					"=============================================MWMR_BASIC===========================================");
			break;
		default:
			assert (false);
			break;
		}

		// ds_ = MWMRHeapUpdateDoublesSketch.newInstance(k_, numberOfThreads,
		// numberOfTreeLevels, numberOfWriters);

		// must warm up!!!
		for (double i = 1; i < 10000000; i++) {
			ds_.update(i);
		}

		LOG.info(ds_.getQuantile(0.5));

	}

	private void runTest(int writersNum, int readersNum, int mixedNum, long secondsToRun) throws Exception {

		TestContext ctx = new TestContext();

		// OperationsNum ops = new OperationsNum();

		List<WriterThread> writersList = Lists.newArrayList();
		for (int i = 0; i < writersNum; i++) {
			WriterThread writer = new WriterThread(ds_);
			writersList.add(writer);
			ctx.addThread(writer);
		}

		List<ReaderThread> readersList = Lists.newArrayList();
		for (int i = 0; i < readersNum; i++) {
			ReaderThread reader = new ReaderThread(ds_);
			readersList.add(reader);
			ctx.addThread(reader);
		}

		List<MixedThread> mixedList = Lists.newArrayList();
		for (int i = 0; i < mixedNum; i++) {
			MixedThread mixed = new MixedThread(ds_);
			mixedList.add(mixed);
			ctx.addThread(mixed);
		}

		ctx.startThreads();
		ctx.waitFor(secondsToRun * 1000);
		ctx.stop();

		long totalReads = 0;
		long totalWrites = 0;

		// LOG.info("Mixed threads:");

		for (MixedThread mixed : mixedList) {
			totalReads += mixed.readOps_;
			totalWrites += mixed.writeOps_;
		}
		// LOG.info("writeTput = " + (totalWrites / secondsToRun));
		// LOG.info("readTput = " + (totalReads / secondsToRun));
		// LOG.info("Total ThPut= " + ((totalReads + totalWrites) / secondsToRun));

		totalReads = 0;
		totalWrites = 0;

		// LOG.info("Write threads:");
		for (WriterThread writer : writersList) {
			totalWrites += writer.operationsNum_;
		}
		LOG.info("writeTput = " + ((totalWrites / secondsToRun)) / 1000000 + " millions per second");

		// LOG.info("Read threads:");
		for (ReaderThread reader : readersList) {
			totalReads += reader.readOperationsNum_;
		}
		LOG.info("readTput = " + ((totalReads / secondsToRun)) / 1000000.0 + " millions per second");

		LOG.info(ds_.getQuantile(0.5));

	}

	public static class WriterThread extends TestThread {
		// Random rand_ = new Random();
		// HeapUpdateDoublesSketch ds_;
		long operationsNum_ = 0;
		// OperationsNum ops_;
		public final Log LOG = LogFactory.getLog(WriterThread.class);

		// public WriterThread(TestContext ctx, HeapUpdateDoublesSketch ds) {
		public WriterThread(HeapUpdateDoublesSketch ds) {
			super(ds);
			// ds_ = ds;
		}

		@Override
		public void doWork() throws Exception {

			// if (!getAffinity_()) {
			// LOG.info( "I am a writer and my core is " + ThreadAffinity.currentCore());
			// long mask = 1 << 2;
			// ThreadAffinity.setCurrentThreadAffinityMask(mask);
			// setAffinity_(true);
			// LOG.info( "I am a writer and my core is " + ThreadAffinity.currentCore());
			// }

			ds_.update(operationsNum_);
			operationsNum_++;
			// assert(operationsNum_ > 0);
		}

	}

	public static class ReaderThread extends TestThread {

		// Random rand_ = new Random();
		// HeapUpdateDoublesSketch ds_;
		long readOperationsNum_ = 0;

		public ReaderThread(HeapUpdateDoublesSketch ds) {
			super(ds);
			// ds_ = ds;

		}

		@Override
		public void doWork() throws Exception {

			// if (!getAffinity_()) {
			// LOG.info( "I am a reader and my core is " + ThreadAffinity.currentCore());
			// long mask = 1 << 5;
			// ThreadAffinity.setCurrentThreadAffinityMask(mask);
			// setAffinity_(true);
			// LOG.info( "I am a reader and my core is " + ThreadAffinity.currentCore());
			// }

			// double[] a = new double[1000];
			//
			// for (int i = 0; i < 1000 ; i++) {
			// a[i] = i;
			// }
			//
			// long endTime = System.currentTimeMillis() + 10;
			// while (true) {
			// long left = endTime - System.currentTimeMillis();
			// if (left <= 0)
			// break;
			// }
			////

			ds_.getQuantile(0.5);
			readOperationsNum_++;
		}
	}

	public static class MixedThread extends TestThread {

		// Random rand_ = new Random();
		// HeapUpdateDoublesSketch ds_;
		double writeOps_ = 0;
		long readOps_ = 0;
		int i_ = 1;

		public MixedThread(HeapUpdateDoublesSketch ds) {
			super(ds);
			// ds_ = ds;
		}

		@Override
		public void doWork() throws Exception {

			if ((i_ % 2) == 0) {
				ds_.getQuantile(0.5);
				readOps_++;
			} else {
				ds_.update(i_);
				writeOps_++;
			}
			i_++;

			// LOG.info("writeOps_:" + this.writeOps_);
		}
	}

}
