/**
 * 
 */
package eu.semagrow.stack.modules.utils.impl;

import eu.semagrow.stack.modules.utils.Measurement;

/* (non-Javadoc)
 * @see eu.semagrow.stack.modules.utils.Measurement
 */
public class MeasurementImpl implements Measurement {
	
	private int id;
	private String type;
	private int value;
	
	/**
	 * @param id
	 * @param type
	 * @param value
	 */
	public MeasurementImpl(int id, String type, int value) {
		super();
		this.id = id;
		this.type = type;
		this.value = value;
	}
	
	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.Measurement#getId()
	 */
	public int getId() {
		return id;
	}
	
	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.Measurement#getType()
	 */
	public String getType() {
		return type;
	}
	
	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.Measurement#getValue()
	 */
	public int getValue() {
		return value;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.Measurement#toString()
	 */
	@Override
	public String toString() {
		return "Measurement [id=" + id + ", type=" + type + ", value=" + value
				+ "]";
	}
	
	

}
