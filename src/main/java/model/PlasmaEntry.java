package model;

import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;

@AllArgsConstructor
public class PlasmaEntry implements Serializable {
    public static final long serialVersionUID = 4328703;

    public String key;
    public byte[] value;
    public byte[] nextPlasmaID;

    @Override
    public String toString() {
        return key + Arrays.toString(value) + Arrays.toString(nextPlasmaID);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof PlasmaEntry entry)) {
            return false;
        }

        return entry.key.equals(key) && Arrays.equals(entry.value, value) && Arrays.equals(entry.nextPlasmaID, nextPlasmaID);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + key.hashCode();
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + Arrays.hashCode(nextPlasmaID);
        return result;
    }
}
