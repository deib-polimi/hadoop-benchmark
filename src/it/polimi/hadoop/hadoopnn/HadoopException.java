package it.polimi.hadoop.hadoopnn;

public class HadoopException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7236254850866269667L;

	public HadoopException() {
	}

	public HadoopException(String message) {
		super(message);
	}

	public HadoopException(Throwable cause) {
		super(cause);
	}

	public HadoopException(String message, Throwable cause) {
		super(message, cause);
	}

	public HadoopException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
