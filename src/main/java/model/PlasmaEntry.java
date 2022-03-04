package model;

import lombok.AllArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
public class PlasmaEntry implements Serializable {
    public String key;
    public byte[] value;
    public byte[] nextPlasmaID;

    @Override
    public String toString() {
        return key + value.toString() + nextPlasmaID.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof PlasmaEntry)) {
            return false;
        }

        PlasmaEntry entry = (PlasmaEntry) o;

        return entry.key.equals(key) && entry.value.equals(value) && entry.nextPlasmaID.equals(nextPlasmaID);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + key.hashCode();
        result = 31 * result + value.hashCode();
        result = 31 * result + nextPlasmaID.hashCode();
        return result;
    }
}
