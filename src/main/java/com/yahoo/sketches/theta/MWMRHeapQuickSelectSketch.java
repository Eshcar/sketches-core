package com.yahoo.sketches.theta;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;

public class MWMRHeapQuickSelectSketch extends HeapQuickSelectSketch {

	private ExecutorService executorService_;
	private volatile double estimation_;
	private volatile long theta_;

	public MWMRHeapQuickSelectSketch(int lgNomLongs, long seed, float p, ResizeFactor rf, int preambleLongs,
			Family family) {
		super(lgNomLongs, seed, p, rf, preambleLongs, family);
	}

	public void prpogate(final Sketch sketchIn, AtomicBoolean full) {
		BackgroundThetaPropogation job = new BackgroundThetaPropogation(sketchIn, full);
		executorService_.execute(job);
	}

	public double getEstimation() {
		return estimation_;
	}
	
	public long getVolatileTheta() {
		return theta_;
	}

	public class BackgroundThetaPropogation implements Runnable {

		Sketch sketch_;
		AtomicBoolean full_;

		BackgroundThetaPropogation(final Sketch sketchIn, AtomicBoolean full) {
			sketch_ = sketchIn;
			full_ = full;
		}

		@Override
		public void run() {
			assert theta_ <= sketch_.getThetaLong();
			
			
			final long[] cacheIn = sketch_.getCache(); 
			for (int i = 0; i < sketch_.getRetainedEntries(true); i++) {
				final long hashIn = cacheIn[i];
				if (hashIn >= theta_) {
					break;
				} // "early stop"
				hashUpdate(hashIn); // backdoor update, hash function is bypassed
			}
			
			
			estimation_ = getEstimate();
			theta_ = getThetaLong();
			
			full_.set(false);
			
		}
	}

}
