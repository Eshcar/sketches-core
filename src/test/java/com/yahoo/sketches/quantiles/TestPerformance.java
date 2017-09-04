package com.yahoo.sketches.quantiles;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.yahoo.sketches.quantiles.ConccurencyFramworkTest.MixedThread;
import com.yahoo.sketches.quantiles.ConccurencyFramworkTest.ReaderThread;
import com.yahoo.sketches.quantiles.ConccurencyFramworkTest.SketchType;
import com.yahoo.sketches.quantiles.ConccurencyFramworkTest.WriterThread;

import utils.ConcurrencyTestUtils.TestContext;
import utils.ConcurrencyTestUtils.TestThread;

public class TestPerformance {
	
	private HeapUpdateDoublesSketch ds_;
//	private static final SketchType type_ = ;
	private final int k_ = 128;
	private final Log LOG = LogFactory.getLog(ConccurencyFramworkTest.class);
	
	
	public static void main(String[] args) throws Exception {
		
		Log LOG1 = LogFactory.getLog(ConccurencyFramworkTest.class);
		
		TestPerformance test = new TestPerformance();
		
		test.setUp();
		test.runTest(1, 0, 0, 60);
		test.setUp();
		test.runTest(1, 0, 0, 60);
		test.setUp();
		test.runTest(1, 0, 0, 60);
		
		
		LOG1.info("read + write");
		
		test.setUp();
		test.runTest(1, 1, 0, 60);
		test.setUp();
		test.runTest(1, 1, 0, 60);
		test.setUp();
		test.runTest(1, 1, 0, 60);
		
		
		
	}
	
	public void setUp() throws Exception {


			ds_ = MWMRHeapUpdateDoublesSketch.newInstance(k_, 1, 2, 1);
	
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

//		LOG.info("Mixed threads:");

		for (MixedThread mixed : mixedList) {
			totalReads += mixed.readOps_;
			totalWrites += mixed.writeOps_;
		}
//		LOG.info("writeTput = " + (totalWrites / secondsToRun));
//		LOG.info("readTput = " + (totalReads / secondsToRun));
//		LOG.info("Total ThPut= " + ((totalReads + totalWrites) / secondsToRun));

		totalReads = 0;
		totalWrites = 0;

//		LOG.info("Write threads:");
		for (WriterThread writer : writersList) {
			totalWrites += writer.operationsNum_;
		}
		LOG.info("writeTput = " + ((totalWrites / secondsToRun)) / 1000000 + " millions per second");

//		LOG.info("Read threads:");
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
