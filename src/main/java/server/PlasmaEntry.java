package server;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@AllArgsConstructor
@Data
public record PlasmaEntry(String key, byte[] value, byte[] nextPlasmaID) implements Serializable {
}
