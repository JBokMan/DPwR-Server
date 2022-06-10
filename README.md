# InfinimumDB-Server

## Prerequisites:

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

1. install the latest version
   of [CMake](https://askubuntu.com/questions/355565/how-do-i-install-the-latest-version-of-cmake-from-the-command-line)
2. run ```sudo apt install python3-numpy```
3. run ```sudo apt-get install build-essential; sudo apt-get install curl; sudo apt-get install zip; sudo apt-get install openjdk-17-jdk; sudo apt-get install default-jdk; sudo apt-get install python3-dev; sudo apt-get install python3-numpy```
4. clone Apache Arrow from [GitHub](https://github.com/apache/arrow)
5. run  ```cd arrow/cpp; mkdir release; cd release```
6. find and open the file *arrow/java/plasma/test.sh*
   - copy the options of the *cmake* found in the test.sh file command
7. run ```sudo cmake ``` followed by the copied options
   - full command should look something like this: 
   ```bash
   sudo cmake -DCMAKE_BUILD_TYPE=Release \
   -DCMAKE_C_FLAGS="-g -O3" \
   -DCMAKE_CXX_FLAGS="-g -O3" \
   -DARROW_BUILD_TESTS=off \
   -DARROW_HDFS=on \
   -DARROW_BOOST_USE_SHARED=on \
   -DARROW_PYTHON=on \
   -DARROW_PLASMA=on \
   -DPLASMA_PYTHON=on \
   -DARROW_JEMALLOC=off \
   -DARROW_WITH_BROTLI=off \
   -DARROW_WITH_LZ4=off \
   -DARROW_WITH_ZLIB=off \
   -DARROW_WITH_ZSTD=off \
   -DARROW_PLASMA_JAVA_CLIENT=on \
   ..

9. run ```sudo make VERBOSE=1 -j4; sudo make install```
10. run ```sudo cp -a release/. /usr/lib```

## How to run the server
After cloning the repository run in the project folder:

```./gradlew shadowJar; export UCX_ERROR_SIGNALS=""; java --add-modules jdk.incubator.foreign --enable-native-access=ALL-UNNAMED -cp "build/libs/InfinimumDB-Server-1.0-SNAPSHOT-all.jar:application.jar" main.Application```

## How to gracefully shut down the server

1. Find the PID with for example ```ps -A | grep java```
2. Run ```kill -s TERM <PID>``` where \<PID\> should be replaced with the correct PID

## Known Bugs/Problems:

### Could NOT find Python3
Following error message appears when running cmake to install Apache Arrow JNI:

```bash
CMake Error at /usr/share/cmake-3.18/Modules/FindPackageHandleStandardArgs.cmake:165 (message): Could NOT find Python3 (missing: Python3_NumPy_INCLUDE_DIRS NumPy) (found version "3.9.7")
```
**Solution:** run ```sudo apt-get install python3-numpy```

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
