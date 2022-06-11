# InfinimumDB-Server

## Prerequisites:

### Pull Submodules
After cloning the repository the submodules must also be pulled
```git submodule update --init --recursive```

### Install Java Panama
- Install sdk-man
1. ```curl -s "https://get.sdkman.io" | bash```
2. ```source "$HOME/.sdkman/bin/sdkman-init.sh"```
- Download panama installer from https://coconucos.cs.hhu.de/forschung/jdk/install and install java
3. ```bash panama-install.sh```
4. ```sdk use java panama```

### Install Python
1. ```curl https://pyenv.run | bash```
2. ```pyenv install 3.8.3```
- Download pip bootstrap from https://bootstrap.pypa.io/get-pip.py and install pip
3. ```python3 get-pip.py```

### Install Plasma server
```pip install pyarrow==8.0.*```

### Install UCX
Install UCX from https://github.com/openucx/ucx/releases/tag/v1.12.1

### Install **Apache Arrow JNI** with make:

1. install the latest version for development
   of [CMake](https://askubuntu.com/questions/355565/how-do-i-install-the-latest-version-of-cmake-from-the-command-line)
2. run ```sudo apt-get install build-essential openjdk-17-jdk default-jdk```
3. clone [Apache Arrow](https://github.com/apache/arrow) from GitHub
4. run  ```cd arrow/cpp; mkdir release; cd release```
5. run
   ```bash
   sudo cmake \ 
   -DCMAKE_BUILD_TYPE=Release \
   -DCMAKE_C_FLAGS="-g -O3" \
   -DCMAKE_CXX_FLAGS="-g -O3" \
   -DARROW_BUILD_TESTS=off \
   -DARROW_HDFS=off \
   -DARROW_BOOST_USE_SHARED=off \
   -DARROW_PYTHON=off \
   -DARROW_PLASMA=on \
   -DPLASMA_PYTHON=off \
   -DARROW_JEMALLOC=off \
   -DARROW_WITH_BROTLI=off \
   -DARROW_WITH_LZ4=off \
   -DARROW_WITH_ZLIB=off \
   -DARROW_WITH_ZSTD=off \
   -DARROW_PLASMA_JAVA_CLIENT=on \
   ..

6. run ```sudo make VERBOSE=1 -j$(nproc)```
7. run ```sudo make install```

## How to run the server
After cloning the repository run:

1. ```./gradlew shadowJar
2. ``` export UCX_ERROR_SIGNALS=""```
3. ``` java --add-modules jdk.incubator.foreign --enable-native-access=ALL-UNNAMED -cp "build/libs/DPwR-Server-1.0-SNAPSHOT-all.jar:application.jar" main.Application --verbose```

## How to gracefully shut down the server

1. STRG + C

Or

1. Find the PID with for example ```ps -A | grep java```
2. Run ```kill -s TERM <PID>``` where \<PID\> should be replaced with the correct PID

## Known Bugs/Problems:

### Gradle File Not Found
Exception in thread "main" java.io.FileNotFoundException: https://downloads.gradle-dn.com/distributions-snapshots/gradle-7.5-20220113232546+0000-bin.zip

Solution
Download gradle nightly from https://gradle.org/nightly/

### Gradle unsupported class file major version
BUG! exception in phase 'semantic analysis' in source unit '_BuildScript_' Unsupported class file major version 63

Solution
```sdk install java 17.0.3.6.1-amzn```
```sdk use java 17.0.3.6.1-amzn```
For building java 17 is required but for running java 19 is required so after building run ```sdk use java panama```

### LibLLVM not found
Exception in thread "main" java.lang.UnsatisfiedLinkError: /home/julian/.sdkman/candidates/java/panama/lib/libclang.so: libLLVM-11.so.1: cannot open shared object file: No such file or directory

Solution
```sudo apt-get install llvm-11```

### UCX not installed
fatal error: 'uct/api/uct.h' file not found

Solution
install ucx follow the prerequisits

### Linkage error class file versions
Error: LinkageError occurred while loading main class main.Application
	java.lang.UnsupportedClassVersionError: main/Application has been compiled by a more recent version of the Java Runtime (class file version 63.0), this version of the Java Runtime only recognizes class file versions up to 61.0

Solution
```sdk use java panama```

### Plasma store not installed
11:26:50.612 ERROR server.PlasmaServer.startProcess() @45 - Cannot run program "plasma_store": error=2, No such file or directory

Solution
```pip install pyarrow==8.0.*```

### Could NOT find Python3
Following error message appears when running cmake to install Apache Arrow JNI:

```bash
CMake Error at /usr/share/cmake-3.18/Modules/FindPackageHandleStandardArgs.cmake:165 (message): Could NOT find Python3 (missing: Python3_NumPy_INCLUDE_DIRS NumPy) (found version "3.9.7")
```
**Solution1:** run ```sudo apt-get install python3-numpy```

**Solution2:** run ```sudo apt-get install python3-dev```

**Solution3:** run build with only ARROW_PLASMA and ARROW_PLASMA_JAVA_CLIENT enabled

### No plasma_java in java.library.path
Following error message appears when starting a server:

```bash
Exception in thread "main" java.lang.UnsatisfiedLinkError: no plasma_java in java.library.path: /usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib
```

**Solution1:** You need to install the Apache Arrow JNI, follow the Prerequisites for that.

**Solution2:** The installed JNI is not found correctly. Add the path to the .../arrow/cpp/release/release folder to the LD_LIBRARY_PATH environment variable and to the java.library.path ```java -Djava.library.path=```

### No stdc++ in java.library.path
Following error message appears when running the server ```java.lang.UnsatisfiedLinkError: no stdc++ in java.library.path:```

**Solution:**
The libstdc++.so is not found in the java.library.path. Run ```find / -name 'libstdc++.so*' 2>/dev/null``` to see if a libstdc++.so exists.
If there is one but it is not named exactly libstdc++.so create a symlink ```ln -s /path/to/libstdc++.so.something /where/you/want/libtsdc++.so```.
Now add the symlink to the LD_LIBRARY_PATH environment variable and when starting the server add it to the library path with ```java -Djava.library.path=```.

If you did not find any libstdc++.so install the package ```libstdc++-10-dev``` via ```sudo apt-get install libstdc++-10-dev``` and you should find one.
