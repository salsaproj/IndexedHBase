package iu.pti.hbaseapp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Range constraint defined by a lower bound value, an upper bound value, and a comparator.
 * @author gaoxm
 *
 * @param <T>
 */
public class RangeIdxConstraint<T> implements IndexDataConstraint<T> {
	protected ArrayList<T> boundaryValues = null;
	protected Comparator<T> comparator = null;
	
	/**
	 * Construct a range constraint using min, max, and cmp as the lower bound, upper bound, and 
	 * comparator, respectively.
	 * @param min
	 * @param max
	 * @param cmp
	 * @throws IllegalArgumentException if cmp is null or min is larger than max.
	 */
	public RangeIdxConstraint(T min, T max, Comparator<T> cmp) throws IllegalArgumentException {
		if (cmp == null) {
			throw new IllegalArgumentException("Invalid Comparator: null.");
		}
		if (min != null && max != null && cmp.compare(min, max) > 0) {
			throw new IllegalArgumentException("Lower bound is larger than upper bound!");
		}
		boundaryValues = new ArrayList<T>(2);
		boundaryValues.add(min);
		boundaryValues.add(max);
		comparator = cmp;
	}

	@Override
	public boolean satisfiedBy(T value) throws Exception {
		T min = boundaryValues.get(0);
		T max = boundaryValues.get(1);
		if (min == null && max == null) {
			return true;
		} else if (min == null) {
			return comparator.compare(value, max) <= 0;
		} else if (max == null) {
			return comparator.compare(min, value) <= 0;
		} else {
			return comparator.compare(min, value) <= 0 && comparator.compare(value, max) <= 0; 
		}
	}

	/**
	 * Get the boundary values for the range constraint.
	 * @return
	 * 	A list of values. The first value (indexed at 0) contains the lower bound and the second value
	 * 	(indexed at 1) contains the upper bound.
	 */
	public List<T> getBoundaryValues() {
		return boundaryValues;
	}
}
