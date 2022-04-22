package utils;

import de.hhu.bsinfo.infinileap.binding.ErrorHandler;
import de.hhu.bsinfo.infinileap.binding.Status;
import jdk.incubator.foreign.MemoryAddress;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DPwRErrorHandler implements ErrorHandler {
    @Override
    public void onError(final MemoryAddress userData, final MemoryAddress endpoint, final Status status) {
        log.error(status.name());
    }
}
