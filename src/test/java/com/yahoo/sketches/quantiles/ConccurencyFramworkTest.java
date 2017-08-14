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

import java.util.List;

@RunWith(Parameterized.class)
public class ConccurencyFramworkTest {

	@Parameterized.Parameters
	public static Object[] data() {
		// return new Object[] { "ORIGINAL", "SWMR_BASIC", "LOCK_BASE_OIGENAL" }; //
		// "ORIGINAL", "SWWR_BASIC",
		// "GLOBAL_LOCK, 'MWMR"

		 return new Object[] {"LOCK_BASE_OIGENAL" };
		// return new Object[] {"ORIGINAL" };
//		return new Object[] { "SWMR_BASIC" };
	}

	enum SketchType {
		// ORIGINAL, SWWR_BASIC, GLOBAL_LOCK, MWMR
		ORIGINAL, SWMR_BASIC, LOCK_BASE_OIGENAL
	}

	enum ThreadType {
		MIXED, FIXED
	}

	private HeapUpdateDoublesSketch ds_;
	private final SketchType type_;
	private static final int k_ = 128;
	private static final Log LOG = LogFactory.getLog(ConccurencyFramworkTest.class);

	public ConccurencyFramworkTest(String type) {
		this.type_ = SketchType.valueOf(type);
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
					"=============================================SWMR_BASIC=============================================");
			break;
		case LOCK_BASE_OIGENAL:
			ds_ = new LockBasedHeapUpdateDoublesSketch(k_);
			LOG.info(
					"=============================================LOCK_BASE_OIGENAL=============================================");
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

	@Test
	public void SWMR() throws Exception {
		LOG.info("=====================Test SWMR:======================");

		switch (type_) {
		case ORIGINAL:
			LOG.info("Type = " + type_ + " Test skipped");
			break;
		default:
			runTest(1, 1, 0, 100);
			ds_.reset();
			runTest(1, 1, 0, 100);
			ds_.reset();
			runTest(1, 1, 0, 100);
			ds_.reset();
			runTest(1, 1, 0, 100);
			ds_.reset();
			runTest(1, 1, 0, 100);
			ds_.reset();
			runTest(1, 1, 0, 100);
			ds_.reset();
			break;
		}
	}

	private void runTest(int writersNum, int readersNum, int mixedNum, long secondsToRun) throws Exception {

		TestContext ctx = new TestContext();

		List<WriterThread> writersList = Lists.newArrayList();
		for (int i = 0; i < writersNum; i++) {
			WriterThread writer = new WriterThread(ctx, ds_);
			writersList.add(writer);
			ctx.addThread(writer);
		}

		List<ReaderThread> readersList = Lists.newArrayList();
		for (int i = 0; i < readersNum; i++) {
			ReaderThread reader = new ReaderThread(ctx, ds_);
			readersList.add(reader);
			ctx.addThread(reader);
		}

		List<MixedThread> mixedList = Lists.newArrayList();
		for (int i = 0; i < mixedNum; i++) {
			MixedThread mixed = new MixedThread(ctx, ds_);
			mixedList.add(mixed);
			ctx.addThread(mixed);
		}

		ctx.startThreads();
		ctx.waitFor(secondsToRun * 1000);
		ctx.stop();

		LOG.info("Finished test. ");

		int totalReads = 0;
		int totalWrites = 0;

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
		LOG.info("writeTput = " + (totalWrites / secondsToRun));

		LOG.info("Read threads:");
		for (ReaderThread reader : readersList) {
			totalReads += reader.operationsNum_;
		}
		LOG.info("readTput = " + (totalReads / secondsToRun));

	}

	public static class WriterThread extends TestThread {
		// Random rand_ = new Random();
		HeapUpdateDoublesSketch ds_;
		int operationsNum_ = 0;

		public WriterThread(TestContext ctx, HeapUpdateDoublesSketch ds) {
			super(ctx);
			this.ds_ = ds;

		}

		@Override
		public void doWork() throws Exception {

			this.ds_.update(operationsNum_);
			this.operationsNum_++;
		}

	}

	public static class ReaderThread extends TestThread {

		// Random rand_ = new Random();
		HeapUpdateDoublesSketch ds_;
		int operationsNum_ = 0;

		public ReaderThread(TestContext ctx, HeapUpdateDoublesSketch ds) {
			super(ctx);
			this.ds_ = ds;
		}

		@Override
		public void doWork() throws Exception {

			this.ds_.getQuantile(0.5);
			this.operationsNum_++;
		}
	}

	public static class MixedThread extends TestThread {

		// Random rand_ = new Random();
		HeapUpdateDoublesSketch ds_;
		double writeOps_ = 0;
		long readOps_ = 0;
		int i_ = 1;

		public MixedThread(TestContext ctx, HeapUpdateDoublesSketch ds) {
			super(ctx);
			this.ds_ = ds;
		}

		@Override
		public void doWork() throws Exception {

			if ((i_ % 20) == 0) {
				this.ds_.getQuantile(0.5);
				this.readOps_++;
			} else {
				this.ds_.update(i_);
				this.writeOps_++;
			}
			i_++;

			// LOG.info("writeOps_:" + this.writeOps_);
		}
	}
}
