package be.bagofwords.miniorm.data;

/**
 * Created by koen on 26/03/17.
 */
public class ReadField {
    public final Object value;
    public final Class<?> type;

    public ReadField(Object value, Class<?> type) {
        this.value = value;
        this.type = type;
    }
}
