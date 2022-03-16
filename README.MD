

# 1.JAVA_HOME
```shell
export JAVA_HOME=/usr/lib/jvm/java
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:$PATH
```
# 2. Build compression
## 1. use rockdb make
```shell
DEBUG_LEVEL=0 make libsnappy.a
DEBUG_LEVEL=0 make libzstd.a
DEBUG_LEVEL=0 make liblz4.a
DEBUG_LEVEL=0 make libbz2.a
DEBUG_LEVEL=0 make libz.a
```
## 2. by yourself

### 1. Snappy
```shell
wget https://src.fedoraproject.org/lookaside/pkgs/snappy/snappy-1.0.5.tar.gz/4c0af044e654f5983f4acbf00d1ac236/snappy-1.0.5.tar.gz
./configure --disable-shared --with-pic
make install
```

### 2. zstd
```shell
wget https://github.com/facebook/zstd/releases/download/v1.5.2/zstd-1.5.2.tar.gz
cmake . 
make libzstd_static

cp zstd.h /usr/local/include/
cp zdict.h /usr/local/include/
cp zstd_errors.h /usr/local/include/
cp libzstd.a /usr/local/lib64/
```

### 3. lz4
```shell
wget https://github.com/lz4/lz4/archive/refs/tags/v1.9.3.tar.gz
make liblz4.a
cp liblz4.a /usr/local/lib64/
cp lz4.h /usr/local/include/
cp lz4frame.h /usr/local/include/
cp lz4hc.h /usr/local/include/

```

### 4. bzip2
```shell
wget wget https://www.sourceware.org/pub/bzip2/bzip2-latest.tar.gz --no-check-certificate
Vim makefile CFLAGS  add -fPIC
make libbz2.a
cp bzlib.h /usr/local/include/
cp libbz2.a /usr/local/lib64/
```

### 5. zlib
```shell
https://www.zlib.net/zlib-1.2.11.tar.gz
./configure
Make 

cp libz.a /usr/local/lib
cp zlib.h zconf.h /usr/local/include
```

#3. MAKE rocksdbJni
```shell
WITH_BZ2=1 WITH_ZLIB=1 WITH_LZ4=1 WITH_ZSTD=1 WITH_SNAPPY=1 PORTABLE=1 DEBUG_LEVEL=0 make -j8 rocksdbjava
```