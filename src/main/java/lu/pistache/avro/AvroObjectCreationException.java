package lu.pistache.avro;

public class AvroObjectCreationException extends Exception {
    public AvroObjectCreationException(String message) {
        super(message);
    }

    public AvroObjectCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
