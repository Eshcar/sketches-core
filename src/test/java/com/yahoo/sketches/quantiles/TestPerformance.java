package com.yahoo.sketches.quantiles;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

import utils.ConcurrencyTestUtils.TestContext;
import utils.ConcurrencyTestUtils.TestThread;

import com.yahoo.sketches.quantiles.HeapUpdateDoublesSketch;
import com.yahoo.sketches.quantiles.Contex;
import com.yahoo.sketches.theta.Sketches;
//import com.yahoo.sketches.quantiles.ConccurencyFramworkTest.SketchType;

public class TestPerformance {

	private MWMRHeapUpdateDoublesSketch ds_;
	// private static final SketchType type_ = ;
	private final int k_ = 4096;
	public final Log LOG = LogFactory.getLog(TestPerformance.class);
	public Logger logger = Logger.getLogger("MyLog");
	public FileHandler fh;
	
//	HeapUpdateDoublesSketch[] sketches_;
	
	
	enum SketchType {
		// ORIGINAL, SWWR_BASIC, GLOBAL_LOCK, MWMR
		ORIGINAL, SWMR_BASIC, LOCK_BASE_OIGENAL, MWMR_BASIC
	}


	public static void main(String[] args) throws Exception {

		TestPerformance test = new TestPerformance();

		try {

			// This block configure the logger with handler and formatter
			test.fh = new FileHandler("/home/sashas/sketches/sketches-core/testlog", true);
			test.logger.addHandler(test.fh);
			SimpleFormatter formatter = new SimpleFormatter();
			test.fh.setFormatter(formatter);

			// the following statement is used to log any messages

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// try {
		// test.LOG.info("I am the main thread and my core is " +
		// ThreadAffinity.currentCore());
		// long mask = 1 << 0;
		// ThreadAffinity.setCurrentThreadAffinityMask(mask);
		// test.LOG.info("I am the main thread and my core is " +
		// ThreadAffinity.currentCore());
		// } catch (Exception e) {
		// // TODO: handle exception
		// test.LOG.info("catched RuntimeException: " + e);
		// }

		int writers = Integer.parseInt(args[0]);
		int numberOfLevels = Integer.parseInt(args[1]);
		int time = Integer.parseInt(args[2]);
		boolean print = Boolean.parseBoolean(args[3]);
		int updateRetio = 50;
		
		
		if (print) {
			test.logger.info("writers = " + writers + ", levels = " + numberOfLevels);
		}

		test.setUp(numberOfLevels);
		test.runTest(writers, 0, 0, time, updateRetio);
		test.prtintDebug(time);
		test.clean();
		
//		test.runPatitionTest(writers, time);

		test.LOG.info("Done!");

		System.exit(0);

	}
	
	void prtintDebug(int time) {
		LOG.info("debug = " + (ds_.getDebug_() / (time * 1000000 )) + " millions per second");
	}

	public void clean() {
		ds_.clean();
	}

	public void setUp(int numberOfLevels) throws Exception {


			ds_ = MWMRHeapUpdateDoublesSketch.newInstance(k_, numberOfLevels);
			LOG.info(
					"=============================================MWMR_BASIC===========================================");


		LOG.info(ds_.getQuantile(0.5));
		
		for (double i = 1; i < 10000000; i++) {
			ds_.update(i);
		}

		LOG.info(ds_.getQuantile(0.5));

	}
	
//	private void runPatitionTest(int writersNum, long secondsToRun)
//			throws Exception {
//
//		TestContext ctx = new TestContext();
//		sketches_ = new HeapUpdateDoublesSketch[writersNum];
//		
//		// OperationsNum ops = new OperationsNum();
//		
//
//		List<WriterThread> writersList = Lists.newArrayList();
//		for (int i = 0; i < writersNum; i++) {
//			sketches_[i] = HeapUpdateDoublesSketch.newInstance(k_);
//			WriterThread writer = new WriterThread(sketches_[i],i);
//			writersList.add(writer);
//			ctx.addThread(writer);
//		}
//
//
//
//		ctx.startThreads();
//		ctx.waitFor(secondsToRun * 1000);
//		ctx.stop();
//
//		
//		long totalWrites = 0;
//
//
//		// LOG.info("Write threads:");
//		for (WriterThread writer : writersList) {
//			totalWrites += writer.operationsNum_;
//		}
//		LOG.info("writeTput = " + ((totalWrites / secondsToRun)) / 1000000 + " millions per second");
//		logger.info("writeTput = " + ((totalWrites / secondsToRun)) / 1000000 + " millions per second");
//
//		
//		for (int i = 0; i < writersNum; i++) {
//			LOG.info(sketches_[i].getQuantile(0.5));
//		}
//
//	}

	private void runTest(int writersNum, int readersNum, int mixedNum, long secondsToRun, int updateRetio)
			throws Exception {

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
			MixedThread mixed = new MixedThread(ds_, updateRetio);
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
		logger.info("writeTput = " + ((totalWrites / secondsToRun)) / 1000000 + " millions per second");

		// LOG.info("Read threads:");
		for (ReaderThread reader : readersList) {
			totalReads += reader.readOperationsNum_;
		}
		LOG.info("readTput = " + ((totalReads / secondsToRun)) / 1000000.0 + " millions per second");

		LOG.info(ds_.getQuantile(0.5));

	}

	public class WriterThread extends TestThread {
		long operationsNum_ = 0;
		public final Log LOG = LogFactory.getLog(WriterThread.class);
		private Contex contex_;

		public WriterThread(MWMRHeapUpdateDoublesSketch ds) {
			super(ds, "WRITER");
			contex_ = new Contex(ds);
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

			
			contex_.update(operationsNum_);
			operationsNum_++;
		}

	}

	public static class ReaderThread extends TestThread {

		long readOperationsNum_ = 0;
		public final Log LOG = LogFactory.getLog(ReaderThread.class);

		public ReaderThread(HeapUpdateDoublesSketch ds) {
			super(ds, "READER");

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

			ds_.getQuantile(0.5);
			readOperationsNum_++;
		}
	}

	public static class MixedThread extends TestThread {

		Random rand_ = new Random();
		int Low_ = 1;
		int High_ = 100;
		int updateRetio_;
		boolean latency_ = false;

		int readTopLatency_;

		double writeOps_ = 0;
		long readOps_ = 0;
		int i_ = 1;

		public MixedThread(HeapUpdateDoublesSketch ds, int updateRetio) {
			super(ds, "MIXED");
			updateRetio_ = updateRetio;
		}

		@Override
		public void doWork() throws Exception {

			int res = rand_.nextInt(High_ - Low_ + 1) + Low_;

			if (res > updateRetio_) {

				if (latency_) {
					long startTime = System.nanoTime();
					ds_.getQuantile(0.5);
					long elapsedTime = System.nanoTime() - startTime;
				} else {
					ds_.getQuantile(0.5);
					readOps_++;
				}
			} else {
				ds_.update(i_);
				writeOps_++;
			}
			i_++;

		}
	}

}
