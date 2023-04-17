# This downloads 32 files at a time. (There is probably a much more efficient way to do this)
sudo aria2c -j 64 -s 1 -x 16 -i file_list.txt -d /das_data/h5files
