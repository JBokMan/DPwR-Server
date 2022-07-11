package model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

@AllArgsConstructor
@NoArgsConstructor
public class PlasmaEntry implements Externalizable {
    public static final long serialVersionUID = 4328703L;

    public String key;
    public byte[] value;
    transient public byte[] nextPlasmaID;

    @Override
    public String toString() {
        return key + Arrays.toString(value) + Arrays.toString(nextPlasmaID);
    }

    @Override
    public boolean equals(final Object o) {
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

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeUTF(key);
        out.write(value);
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException {
        this.key = in.readUTF();
        final int size = in.available();
        this.value = new byte[size];
        for (int i = 0; i < size; i++) {
            this.value[i] = in.readByte();
        }
    }
}
