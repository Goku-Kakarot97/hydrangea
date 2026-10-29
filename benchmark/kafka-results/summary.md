### connect samples=30
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| p50_ms | 10 | 13.6 | 3/3 |
| p99_ms | 12.6 | 27.3 | 3/3 |
| rss_mb | 117 | 156 | 3/3 |

### consume size=100 records=3000000
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 5.64367e+06 | 3.85259e+06 | 3/3 |
| mb_per_s | 538.2 | 367.4 | 3/3 |
| client_cpu_s_per_100k | 0.022 | 0.032 | 3/3 |
| threads_peak | 11 | 9 | 3/3 |
| rss_mb | 407 | 453 | 3/3 |

### consume size=1024 records=500000
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 730044 | 692715 | 3/3 |
| mb_per_s | 712.9 | 676.5 | 3/3 |
| client_cpu_s_per_100k | 0.16 | 0.225 | 3/3 |
| threads_peak | 11 | 9 | 3/3 |
| rss_mb | 333 | 299 | 3/3 |

### consume-group size=100 records=3000000
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 6.07469e+06 | 2.97389e+06 | 3/3 |
| mb_per_s | 579.3 | 283.6 | 3/3 |
| client_cpu_s_per_100k | 0.023 | 0.026 | 3/3 |
| threads_peak | 11 | 9 | 3/3 |
| rss_mb | 509 | 471 | 3/3 |

### e2e samples=3000
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| p50_ms | 0.3 | 0.5 | 3/3 |
| p99_ms | 1.2 | 1.7 | 3/3 |
| p999_ms | 3.5 | 3.5 | 3/3 |
| rss_mb | 168 | 218 | 3/3 |

### produce size=100 acks=-1 idem=true inflight=5 partitions=1
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 1.29352e+06 | 1.49063e+06 | 3/3 |
| mb_per_s | 123.4 | 142.2 | 3/3 |
| p50_ms | 27.3 | 28.3 | 3/3 |
| p99_ms | 243.3 | 83.9 | 3/3 |
| client_cpu_s_per_100k | 0.048 | 0.11 | 3/3 |
| caller_cpu_s_per_100k | 0.017 | 0.047 | 3/3 |
| io_cpu_s_per_100k | 0.026 | 0.048 | 3/3 |
| threads_peak | 10 | 7 | 3/3 |
| rss_mb | 246 | 326 | 3/3 |

### produce size=100 acks=-1 idem=true inflight=5 partitions=3
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 1.92838e+06 | 1.61132e+06 | 3/3 |
| mb_per_s | 183.9 | 153.7 | 3/3 |
| p50_ms | 21 | 26.2 | 3/3 |
| p99_ms | 96.5 | 83.9 | 3/3 |
| client_cpu_s_per_100k | 0.043 | 0.108 | 3/3 |
| caller_cpu_s_per_100k | 0.015 | 0.047 | 3/3 |
| io_cpu_s_per_100k | 0.022 | 0.046 | 3/3 |
| threads_peak | 10 | 7 | 3/3 |
| rss_mb | 260 | 357 | 3/3 |

### produce size=100 acks=1 idem=false inflight=1 partitions=3
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 995286 | 1.19275e+06 | 3/3 |
| mb_per_s | 94.9 | 113.7 | 3/3 |
| p50_ms | 46.1 | 35.7 | 3/3 |
| p99_ms | 113.2 | 109.1 | 3/3 |
| client_cpu_s_per_100k | 0.067 | 0.105 | 3/3 |
| caller_cpu_s_per_100k | 0.021 | 0.044 | 3/3 |
| io_cpu_s_per_100k | 0.037 | 0.046 | 3/3 |
| threads_peak | 10 | 7 | 3/3 |
| rss_mb | 248 | 321 | 3/3 |

### produce size=100 acks=1 idem=false inflight=5 partitions=3
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 2.06676e+06 | 1.53749e+06 | 3/3 |
| mb_per_s | 197.1 | 146.6 | 3/3 |
| p50_ms | 19.9 | 26.2 | 3/3 |
| p99_ms | 83.9 | 104.9 | 3/3 |
| client_cpu_s_per_100k | 0.041 | 0.107 | 3/3 |
| caller_cpu_s_per_100k | 0.015 | 0.045 | 3/3 |
| io_cpu_s_per_100k | 0.021 | 0.046 | 3/3 |
| threads_peak | 10 | 7 | 3/3 |
| rss_mb | 259 | 341 | 3/3 |

### produce size=1024 acks=-1 idem=true inflight=5 partitions=3
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 257324 | 227086 | 3/3 |
| mb_per_s | 251.3 | 221.8 | 3/3 |
| p50_ms | 176.2 | 192.9 | 3/3 |
| p99_ms | 503.3 | 570.4 | 3/3 |
| client_cpu_s_per_100k | 0.165 | 0.278 | 3/3 |
| caller_cpu_s_per_100k | 0.023 | 0.071 | 3/3 |
| io_cpu_s_per_100k | 0.105 | 0.119 | 3/3 |
| threads_peak | 10 | 7 | 3/3 |
| rss_mb | 285 | 235 | 3/3 |

### produce-threads acks=-1 threads=1000
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 722952 | 358674 | 3/3 |
| p50_ms | 1.2 | 2.2 | 3/3 |
| p99_ms | 4.5 | 10 | 3/3 |
| client_cpu_s_per_100k | 0.3 | 0.8 | 3/3 |
| threads_peak | 17 | 17 | 3/3 |
| rss_mb | 220 | 247 | 3/3 |

### produce-threads acks=1 threads=1000
| metric | hydrangea | kafka-clients | runs |
|---|---|---|---|
| per_s | 669854 | 336745 | 3/3 |
| p50_ms | 1.2 | 2.4 | 3/3 |
| p99_ms | 5.5 | 11.5 | 3/3 |
| client_cpu_s_per_100k | 0.3 | 0.8 | 3/3 |
| threads_peak | 17 | 17 | 3/3 |
| rss_mb | 213 | 244 | 3/3 |

