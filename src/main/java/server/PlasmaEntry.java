package server;

import lombok.AllArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
public class PlasmaEntry implements Serializable {
    public String key;
    public byte[] value;
    public byte[] nextPlasmaID;
}
