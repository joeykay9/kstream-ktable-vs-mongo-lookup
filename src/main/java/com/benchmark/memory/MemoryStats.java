package com.benchmark.memory;

public record MemoryStats(long heapUsedMb, long rocksdbBlockCacheMb, long rocksdbLiveDataMb, long rssMb) {}
