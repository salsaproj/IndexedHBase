package iu.pti.hbaseapp;

import java.util.Set;

import org.apache.hadoop.io.BytesWritable;

/**
 * Index table operator interface. Given a set of constraints on an index table, an index operator applies the constraints on the
 * corresponding index table, and return required information contained in the rows and cell values that qualify the constraints.  
 * @author gaoxm
 */
public interface IndexTableOperator {
	/**
	 * This function must be called after constraints are set on the index table. It applies the constraints, and return the required 
	 * table elements, i.e. rowkeys, qualifiers, cell values, from the qualified results.
	 * @return
	 * @throws Exception in case "return:ts" is specified as the return constraint.
	 */
	public Set<BytesWritable> getQueriedTableElements() throws Exception;
	
	/**
	 * This function must be called after constraints are set on the index table. It applies the constraints, and return timestamps
	 * from the qualified results.
	 * @return
	 * @throws Exception in case "return:rowkey", "return:qual", or "return:cellval" is specified as the return constraint.
	 */
	public Set<Long> getQueriedTimestamps() throws Exception;
	
	/**
	 * Set constraints on the rowkey, column family, qualifier, timestamp, and cell values. Each constraint string must be given in
	 * this format: <b>CST_TARGET:CST_BODY</b>. <b>CST_TARGET</b> identifies the table element that this constraint will be applied against.
	 * It must be one of "<b>rowkey</b>", "<b>cf</b>", "<b>qual</b>", "<b>ts</b>", "<b>cellval</b>", and "<b>return</b>". <b>CST_BODY</b> 
	 * encodes the actual constraint. When <b>CST_TARGET</b> is "<b>rowkey</b>", "<b>cf</b>", "<b>qual</b>", "<b>ts</b>", or "<b>cellval</b>", 
	 * <b>CST_BODY</b> could be given in one of the following formats:
	 * <p/>
	 * (1) {val1,val2,...} for value set constraint;
	 * <br/> (2) [lower,upper] for range constraint;
	 * <br/> (3) < regular expression > for regular expression constraint;
	 * <br/> (4) ~prefix*~ or ~prefix?~ for prefix constraint.
	 * <p/> 
	 * In addition, it is required that the "<b>ts</b>" constraint must be a value set constraint or range constraint, because it will be
	 * applied on the timestamps of the index table.
	 * <p/>
	 * When <b>CST_TARGET</b> is "<b>return</b>", <b>CST_BODY</b> must be one of "<b>rowkey</b>", "<b>qual</b>", "<b>ts</b>", or "<b>cellval</b>",
	 * because it specifies what need to be returned as the query results after the other constraints are applied on the index table.
	 * 
	 * @param constraints
	 * 	An array of constraints strings.
	 * 
	 * @throws Exception
	 * 	If any errors are encountered when parsing or setting the constraints. 
	 */
	public void setConstraints(String[] constraints) throws Exception;
}