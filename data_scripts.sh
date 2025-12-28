#!/bin/bash
export PYTHONPATH=$(pwd)
python src/data/main.py 20240902 20240908 15m && \
python src/data/main.py 20240909 20240915 15m && \
python src/data/main.py 20240916 20240922 15m && \
python src/data/main.py 20240923 20240929 15m && \
python src/data/main.py 20240930 20241006 15m && \
python src/data/main.py 20241007 20241013 15m && \
python src/data/main.py 20241014 20241020 15m && \
python src/data/main.py 20241021 20241027 15m && \
python src/data/main.py 20241028 20241103 15m && \
python src/data/main.py 20241104 20241110 15m && \
python src/data/main.py 20241111 20241117 15m && \
python src/data/main.py 20241118 20241124 15m && \
python src/data/main.py 20241125 20241201 15m && \
python src/data/main.py 20241202 20241208 15m && \
python src/data/main.py 20241209 20241215 15m && \
python src/data/main.py 20241216 20241222 15m && \
python src/data/main.py 20241223 20241229 15m && \
python src/data/main.py 20241230 20250105 15m && \
python src/data/main.py 20250106 20250112 15m && \
python src/data/main.py 20250113 20250119 15m && \
python src/data/main.py 20250120 20250126 15m && \
python src/data/main.py 20250127 20250202 15m && \
python src/data/main.py 20250203 20250209 15m && \
python src/data/main.py 20250210 20250216 15m && \
python src/data/main.py 20250217 20250223 15m && \
python src/data/main.py 20250224 20250302 15m && \
python src/data/main.py 20250303 20250309 15m && \
python src/data/main.py 20250310 20250316 15m && \
python src/data/main.py 20250317 20250323 15m && \
python src/data/main.py 20250324 20250330 15m && \
python src/data/main.py 20250331 20250406 15m && \
python src/data/main.py 20250407 20250413 15m && \
python src/data/main.py 20250414 20250420 15m && \
python src/data/main.py 20250421 20250427 15m && \
python src/data/main.py 20250428 20250504 15m && \
python src/data/main.py 20250505 20250511 15m && \
python src/data/main.py 20250512 20250518 15m && \
python src/data/main.py 20250519 20250525 15m && \
python src/data/main.py 20250526 20250601 15m && \
python src/data/main.py 20250602 20250608 15m && \
python src/data/main.py 20250609 20250615 15m && \
python src/data/main.py 20250616 20250622 15m && \
python src/data/main.py 20250623 20250629 15m && \
python src/data/main.py 20250630 20250706 15m && \
python src/data/main.py 20250707 20250713 15m && \
python src/data/main.py 20250714 20250720 15m && \
python src/data/main.py 20250721 20250727 15m && \
python src/data/main.py 20250728 20250803 15m && \
python src/data/main.py 20250804 20250810 15m && \
python src/data/main.py 20250811 20250817 15m && \
python src/data/main.py 20250818 20250824 15m && \
python src/data/main.py 20250825 20250831 15m && \
python src/data/main.py 20250901 20250907 15m && \
python src/data/main.py 20250908 20250914 15m && \
python src/data/main.py 20250915 20250921 15m && \
python src/data/main.py 20250922 20250928 15m && \
python src/data/main.py 20250929 20251005 15m && \
python src/data/main.py 20251006 20251012 15m && \
python src/data/main.py 20251013 20251019 15m && \
python src/data/main.py 20251020 20251026 15m && \
python src/data/main.py 20251027 20251102 15m && \
python src/data/main.py 20251103 20251109 15m && \
python src/data/main.py 20251110 20251116 15m && \
python src/data/main.py 20251117 20251123 15m && \
python src/data/main.py 20251124 20251130 15m && \
python src/data/main.py 20251201 20251207 15m && \
python src/data/main.py 20251208 20251214 15m && \
python src/data/main.py 20251215 20251221 15m && \
python src/data/csv_to_pq.py 20240908 20251231 15m && \
python src/data/remove_used_file.py 15m


#export PYTHONPATH=$(pwd)&&python src/data/csv_to_pq.py 20240101 20240908 15m
#python src/data/remove_used_file.py 15m => 날리고 9월부터 다시 받기