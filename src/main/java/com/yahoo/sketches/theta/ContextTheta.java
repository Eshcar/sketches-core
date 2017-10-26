package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ContextTheta extends AContex implements Theta {

	private AtomicBoolean full_ = new AtomicBoolean(false);
	private HeapQuickSelectSketch localSketch_;
	private MWMRHeapQuickSelectSketch shaerdSketch_;
	
	public final Log LOG = LogFactory.getLog(MWMRHeapQuickSelectSketch.class);

	public ContextTheta(MWMRHeapQuickSelectSketch sketch) {

		shaerdSketch_ = sketch;
		UpdateSketchBuilder usb = new UpdateSketchBuilder();
		usb.setNominalEntries(16);
		localSketch_ = (HeapQuickSelectSketch)usb.build();
		localSketch_.setThetaLong(sketch.getVolatileTheta());

	}

	public double getEstimate() {
		return shaerdSketch_.getEstimation();
	}

	public UpdateReturnState update(final long datum) {
		
		
		UpdateReturnState ret = localSketch_.update(datum);
		if ((localSketch_.getRetainedEntries() + 1 > localSketch_.getHashTableThreshold())
				&& localSketch_.getLgArrLongs() > localSketch_.getLgNomLongs()) { //Theta is going to be changed.
			propogateToSharedSketch(datum);
		}
		
		return ret;

	}
	
	private void propogateToSharedSketch(final long dataItem) {
		
		while (full_.get()) {}
		
		HeapCompactOrderedSketch compactSketch = new HeapCompactOrderedSketch(localSketch_);
		full_.set(true);
		
		shaerdSketch_.prpogate(compactSketch, full_);
		localSketch_.reset();
//		System.out.println(shaerdSketch_.getVolatileTheta());
		localSketch_.setThetaLong(shaerdSketch_.getVolatileTheta());
	}

}
