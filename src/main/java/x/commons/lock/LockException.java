package x.commons.lock;

@SuppressWarnings("serial")
public class LockException extends RuntimeException {

	public LockException() {
		super();
	}
	
	public LockException(String s) {
		super(s);
	}
	
	public LockException(Throwable t) {
		super(t);
	}
}
