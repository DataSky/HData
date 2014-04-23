package opensource.hdata.exception;

public class HDataException extends RuntimeException {

    private static final long serialVersionUID = 2510267358921118998L;

    private String message;

    public HDataException() {
        super();
    }

    public HDataException(final String message) {
        super(message);
    }

    public HDataException(final Exception e) {
        super(e);
    }

    public HDataException(Throwable cause) {
        super(cause);
    }

    public HDataException(final String message, final Throwable cause) {
        super(message, cause);
    }

    @Override
    public String getMessage() {
        return this.message == null ? super.getMessage() : this.message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return this.message;
    }

}
