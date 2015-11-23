package iu.pti.hbaseapp;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A constraint defined by a set of valid values. The constraint is satisfied if
 * and only if a given value is a member of the set of valid values. 
 * @author gaoxm
 *
 * @param <T>
 */
public class ValueSetIdxConstraint<T> implements IndexDataConstraint<T> {
	protected Set<T> valueSet = null;
	
	public ValueSetIdxConstraint() {
		valueSet = new HashSet<T>();
	}
	
	/**
	 * Add one valid value to the set of valid values.
	 * @param value
	 */
	public void addValue(T value) {
		valueSet.add(value);
	}
	
	/**
	 * Add all elements of values to the set of valid values.
	 * @param values
	 */
	public void addAllValues(Collection<T> values) {
		valueSet.addAll(values);
	}

	@Override
	public boolean satisfiedBy(T value) throws Exception {
		// TODO Auto-generated method stub
		return valueSet.contains(value);
	}

	/**
	 * Get the set of 'qualified' values in this constraint.
	 * @return
	 */
	public Set<T> getValueSet() {
		// TODO Auto-generated method stub
		return valueSet;
	}
}
