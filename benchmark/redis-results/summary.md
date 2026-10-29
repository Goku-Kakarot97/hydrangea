### bulk-reply-hgetall threads=1
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 1323.9 | 1391.5 | 1482.9 |
| p50_us | 688 | 688 | 589 |
| p99_us | 1769 | 1769 | 1638 |
| p999_us | 4456 | 4456 | 4456 |
| client_cpu_s_per_100k | 33 | 29.5 | 25.4 |
| setup_s | 0 | 0 | 0 |
| threads_peak | 11 | 11 | 8 |
| rss_mb | 220 | 220 | 217 |

### bulk-reply-lrange threads=1
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 434.9 | 440.4 | 460.4 |
| p50_us | 2031 | 1966 | 1769 |
| p99_us | 5767 | 5767 | 5767 |
| p999_us | 7864 | 7340 | 7077 |
| client_cpu_s_per_100k | 113.4 | 112.8 | 95.1 |
| setup_s | 0 | 0 | 0.1 |
| threads_peak | 11 | 11 | 8 |
| rss_mb | 220 | 213 | 219 |

### bulk-reply-lrange threads=8
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 714.4 | 748.2 | 754.9 |
| p50_us | 11010 | 10485 | 10485 |
| p99_us | 19922 | 20971 | 20971 |
| p999_us | 29360 | 32505 | 26214 |
| client_cpu_s_per_100k | 63.7 | 65.8 | 54.5 |
| setup_s | 0 | 0.1 | 0.1 |
| threads_peak | 18 | 25 | 15 |
| rss_mb | 214 | 219 | 223 |

### bulk-reply-mget threads=1
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 2349.9 | 2254 | 2354.2 |
| p50_us | 360 | 376 | 360 |
| p99_us | 1245 | 1310 | 1310 |
| p999_us | 2752 | 2883 | 3407 |
| client_cpu_s_per_100k | 6.9 | 7.3 | 6.8 |
| setup_s | 0 | 0 | 0 |
| threads_peak | 11 | 11 | 8 |
| rss_mb | 216 | 216 | 218 |

### connect connections=500
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 665.6 | 690 | 1094.6 |
| p50_us | 1179 | 1179 | 753 |
| p99_us | 3932 | 3145 | 2097 |
| client_cpu_ms_per_conn | 0.7 | 0.6 | 0.3 |
| threads_peak | 10 | 10 | 7 |
| rss_mb | 66 | 66 | 84 |

### get threads=1
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 3131.9 | 3153.3 | 3293 |
| p50_us | 278 | 262 | 262 |
| p99_us | 1114 | 1179 | 1114 |
| p999_us | 1572 | 1835 | 1507 |
| client_cpu_s_per_100k | 4.5 | 4.5 | 3.1 |
| setup_s | 0 | 0 | 0 |
| threads_peak | 11 | 11 | 8 |
| rss_mb | 83 | 82 | 91 |

### get threads=32
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 65961.3 | 30237.1 | 30964.3 |
| p50_us | 409 | 950 | 917 |
| p99_us | 1310 | 2621 | 2490 |
| p999_us | 2228 | 6291 | 6553 |
| client_cpu_s_per_100k | 1.3 | 2.4 | 2.2 |
| setup_s | 0 | 0 | 0.1 |
| threads_peak | 42 | 49 | 39 |
| rss_mb | 209 | 175 | 179 |

### large size_kb=1
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| write_mb_per_s | 2.9 | 3 | 3.1 |
| read_mb_per_s | 2.9 | 3.1 | 3.2 |
| client_cpu_s_per_gb | 84.8 | 83.8 | 50.6 |
| rss_mb | 61 | 61 | 76 |

### large size_kb=100
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| write_mb_per_s | 67.6 | 66.6 | 69 |
| read_mb_per_s | 86.6 | 84.8 | 85.1 |
| client_cpu_s_per_gb | 2.6 | 2.5 | 1.9 |
| rss_mb | 169 | 169 | 176 |

### large size_kb=1024
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| write_mb_per_s | 114.1 | 111.1 | 113.4 |
| read_mb_per_s | 148.3 | 152.6 | 159.4 |
| client_cpu_s_per_gb | 1.3 | 1.3 | 1.4 |
| rss_mb | 118 | 118 | 134 |

### large size_kb=8192
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| write_mb_per_s | 124.5 | 125 | 123.9 |
| read_mb_per_s | 167.6 | 165.8 | 167 |
| client_cpu_s_per_gb | 0.8 | 0.8 | 0.8 |
| rss_mb | 159 | 159 | 173 |

### pipeline window=100
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 176350 | 176601 | 174647 |
| client_cpu_s_per_100k | 0.1 | 0.1 | 0.1 |
| threads_peak | 10 | 10 | 6 |
| rss_mb | 213 | 212.5 | 211.5 |

### pipeline window=1000
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 623963 | 630753 | 493533 |
| client_cpu_s_per_100k | 0.1 | 0.1 | 0 |
| threads_peak | 10 | 10 | 6 |
| rss_mb | 214 | 213 | 215 |

### pubsub
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| published_per_s | 139209 | 141582 | 140111 |
| received_per_s | 139209 | 141582 | 140111 |
| client_cpu_s_per_100k | 0.2 | 0.2 | 0.1 |
| threads_peak | 10 | 10 | 7 |
| rss_mb | 213 | 213 | 215 |

### tx threads=8
| metric | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| per_s | 21530.7 | 10489.9 | 5877.2 |
| p50_us | 311 | 622 | 1179 |
| p99_us | 1245 | 1638 | 2359 |
| p999_us | 1769 | 3801 | 6029 |
| client_cpu_s_per_100k | 1.7 | 3.1 | 4.9 |
| setup_s | 0 | 0 | 0.1 |
| threads_peak | 18 | 25 | 15 |
| rss_mb | 209 | 174 | 162 |

