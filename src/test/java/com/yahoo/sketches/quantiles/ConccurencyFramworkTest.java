package com.yahoo.sketches.quantiles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.Before;

import utils.ConcurrencyTestUtils.TestContext;
import utils.ConcurrencyTestUtils.TestThread;

import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;

import org.junit.runner.RunWith;

import java.io.ObjectOutputStream.PutField;
import java.util.List;

@RunWith(Parameterized.class)
public class ConccurencyFramworkTest {

	@Parameterized.Parameters
	public static Object[] data() {
		// return new Object[] { "ORIGINAL", "SWMR_BASIC", "LOCK_BASE_OIGENAL" }; //
		// "ORIGINAL", "SWWR_BASIC",
		// "GLOBAL_LOCK, 'MWMR"

		// return new Object[] {"LOCK_BASE_OIGENAL" };
		// return new Object[] {"ORIGINAL" };
		// return new Object[] { "SWMR_BASIC" };
		return new Object[] { "MWMR_BASIC" };
	}

	enum SketchType {
		// ORIGINAL, SWWR_BASIC, GLOBAL_LOCK, MWMR
		ORIGINAL, SWMR_BASIC, LOCK_BASE_OIGENAL, MWMR_BASIC
	}

	enum ThreadType {
		MIXED, FIXED
	}

	private HeapUpdateDoublesSketch ds_;
	private final SketchType type_;
	private static final int k_ = 128;
	private static final Log LOG = LogFactory.getLog(ConccurencyFramworkTest.class);

	public ConccurencyFramworkTest(String type) {
		type_ = SketchType.valueOf(type);
	}

	@Before
	public void setUp() throws Exception {

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
			ds_ = MWMRHeapUpdateDoublesSketch.newInstance(k_, 1, 2, 1);
			LOG.info(
					"=============================================MWMR_BASIC===========================================");
			break;
		default:
			assert (false);
			break;
		}

		// for (double i = 1; i < 100000; i++) {
		// ds_.update(i);
		// }
	}

	// @Test
	// public void testSequentualThread() throws Exception {
	// LOG.info("=============testSequentualThread:==================");
	// runTest(0, 0, 1, 100);
	// ds_.reset();
	// runTest(0, 0, 1, 100);
	// ds_.reset();
	// runTest(0, 0, 1, 100);
	// ds_.reset();
	// runTest(0, 0, 1, 100);
	// ds_.reset();
	// runTest(0, 0, 1, 100);
	// ds_.reset();
	// runTest(0, 0, 1, 100);
	// ds_.reset();
	//
	// }

	// @Test
	// public void SWMR() throws Exception {
	// LOG.info("=====================Test SWMR:======================");
	//
	// switch (type_) {
	// case ORIGINAL:
	// LOG.info("Type = " + type_ + " Test skipped");
	// break;
	// default:
	// runTest(1, 1, 0, 10);
	// ds_.reset();
	// runTest(1, 1, 0, 10);
	// ds_.reset();
	// runTest(1, 1, 0, 10);
	// ds_.reset();
	// runTest(1, 1, 0, 10);
	// ds_.reset();
	// runTest(1, 1, 0, 10);
	// ds_.reset();
	// runTest(1, 1, 0, 10);
	// ds_.reset();
	// break;
	// }
	// }

	@Test
	public void MWMR() throws Exception {
		LOG.info("=====================Test MWMR:======================");

		switch (type_) {
		case ORIGINAL:
		case SWMR_BASIC:
			LOG.info("Type = " + type_ + " Test skipped");
			break;
		default:
			runTest(1, 0, 0, 10);
			setUp();
			runTest(1, 0, 0, 10);
			setUp();
			runTest(1, 0, 0, 10);
			setUp();
			runTest(1, 0, 0, 10);
			setUp();
			LOG.info("writer idle = " + ds_.getDebug_());
		}
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

		LOG.info("Mixed threads:");

		for (MixedThread mixed : mixedList) {
			// LOG.info(" writeTput: " + mixed.writeTput_);
			// LOG.info(" readLatency: " + mixed.readLatency_);
			totalReads += mixed.readOps_;
			totalWrites += mixed.writeOps_;
		}
		LOG.info("writeTput = " + (totalWrites / secondsToRun));
		LOG.info("readTput = " + (totalReads / secondsToRun));
		LOG.info("Total ThPut= " + ((totalReads + totalWrites) / secondsToRun));

		totalReads = 0;
		totalWrites = 0;

		LOG.info("Write threads:");
		for (WriterThread writer : writersList) {
			totalWrites += writer.operationsNum_;
		}
		LOG.info("writeTput = " + ((totalWrites / secondsToRun)) / 1000000 + " millions per second");

		LOG.info("Read threads:");
		for (ReaderThread reader : readersList) {
			totalReads += reader.readOperationsNum_;
		}
		LOG.info("readTput = " + ((totalReads / secondsToRun)) / 1000000.0 + " millions per second");

		LOG.info(ds_.getQuantile(0.5));

		// LOG.info("writeOps = " + ops.writeOp / 1000000.0);
		// LOG.info("readOps = " + ops.readOp / 1000000.0);
	}

	// public static class OperationsNum {
	//
	// public long readOp = 0;
	// public long writeOp = 0;
	//
	//
	// }

	public static class WriterThread extends TestThread {
		// Random rand_ = new Random();
		// HeapUpdateDoublesSketch ds_;
		long operationsNum_ = 0;
		// OperationsNum ops_;

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

			// ds_.update(operationsNum_);
			// LOG.info("Writing. ");
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

			// ds_.getQuantile(0.5);
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
