package iu.pti.hbaseapp;

/**
 * Representing a constraint on any part of the index data. Given a value about the index data,
 * the constraint should be able to check whether the value satisfies this constraint.  
 * @author gaoxm
 *
 * @param <T>
 */
public interface IndexDataConstraint<T> {
	/**
	 * Check if the given value satisfies this constraint. 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public abstract boolean satisfiedBy(T value) throws Exception;
}