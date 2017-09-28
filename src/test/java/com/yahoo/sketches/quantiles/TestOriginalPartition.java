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
import utils.ConcurrencyTestUtils.TestThreadOriginal;

public class TestOriginalPartition {

	public LockBasedHeapUpdateDoublesSketch[] sketches_;

	private final int k_ = 4096;
	public final Log LOG = LogFactory.getLog(TestPerformance.class);
	public Logger logger = Logger.getLogger("MyLog");
	public FileHandler fh;

	public static void main(String[] args) throws Exception {

		TestOriginalPartition test = new TestOriginalPartition();

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

		int writers = Integer.parseInt(args[0]);
		int time = Integer.parseInt(args[1]);
		boolean print = Boolean.parseBoolean(args[2]);
		
		if (print) {
			test.logger.info("writers = " + writers);
		}

		test.setUp(writers);
		test.runTest(writers, time);

	}

	public void setUp(int writers) {

		sketches_ = new LockBasedHeapUpdateDoublesSketch[writers];
		for (int i = 0; i < writers; i++) {
			sketches_[i] = new LockBasedHeapUpdateDoublesSketch(k_);
			for (double j = 1; j < 10000000; j++) {
				sketches_[i].update(j);
			}
		}

	}

	private void runTest(int writersNum, long secondsToRun) throws Exception {

		TestContext ctx = new TestContext();

		List<OriginalWriterThread> writersList = Lists.newArrayList();
		for (int i = 0; i < writersNum; i++) {
			OriginalWriterThread writer = new OriginalWriterThread(writersNum, sketches_[i]);
			writersList.add(writer);
			ctx.addThread(writer);
		}

		ctx.startThreads();
		ctx.waitFor(secondsToRun * 1000);
		ctx.stop();

		long totalWrites = 0;

		// LOG.info("Write threads:");
		for (OriginalWriterThread writer : writersList) {
			totalWrites += writer.operationsNum_;
		}

		logger.info("writeTput = " + ((totalWrites / secondsToRun)) / 1000000 + " millions per second");
		
		for (int i = 0; i < writersNum; i++) {
			LOG.info(sketches_[i].getQuantile(0.5));
		}

	}

	private class OriginalWriterThread extends TestThreadOriginal {
		long operationsNum_ = 0;
		Random rand_ = new Random();
		int accessPathSize_;
		int numberOfSketches_;
//		LockBasedHeapUpdateDoublesSketch sketch_;
		int[] accessPath_;
		int index_ = 0;
//		public final Log LOG = LogFactory.getLog(OriginalWriterThread.class);

		public OriginalWriterThread(int num, LockBasedHeapUpdateDoublesSketch sketch) {
			super(null, "WRITER");
			accessPathSize_ = num * 1000;
//			sketch_ = sketch;
			numberOfSketches_ = num;
			accessPath_ = new int[accessPathSize_];
			for (int i = 0; i < accessPathSize_; i++) {
				accessPath_[i] = rand_.nextInt(numberOfSketches_);
			}
			
		}

		@Override
		public void doWork() throws Exception {

//			int i = rand_.nextInt(numberOfSketches_);
			
//			LOG.info("random number is " + i + ". mu id is " + Thread.currentThread().getId());
			sketches_[accessPath_[index_]].update(operationsNum_);
			index_++;
			if (index_ == accessPathSize_) {
				index_ = 0;
			}
			operationsNum_++;
		}

	}

}
