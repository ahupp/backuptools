
import os
import sys
import zipfile
import re
import shutil
import zipfile
from time import time

# TODO: delete files when copy is done
# TODO: error if last date is too long ago

def validate_and_get_timestamp(src_files):
  zip_ts_candidates = set()
  part_num = []

  ts_re = re.compile(".*/takeout-([0-9TZ]*)-\d+\.zip$")
  part_num_re = re.compile(".*/.*-(\d{3})\.\w+$")

  for fname in src_files:
    if fname.endswith(".zip"):
      ts = ts_re.match(fname)
      if not ts:
        sys.exit("Zip file without timestamp: {}".format(fname))
      zip_ts_candidates.add(ts.group(1))

    m = part_num_re.match(fname)
    if not m:
      sys.exit("File did not have part number: {}".format(fname))
    part_num.append(int(m.group(1)))

  part_num.sort()
  for i in range(1, len(part_num)):
    if (part_num[i] - part_num[i - 1]) != 1:
      sys.exit("Part is missing")

  if len(zip_ts_candidates) == 0:
    if len(src_files) > 0:
      # If we're very unlucky we'll race with the sync into Google Drive, process
      # a bunch of zip files and then on the next run only see the non-timestamped
      # files.  Probably safe to just copy them into the most recent timestamp
      # but lets be conservative.
      sys.exit("Found source files, but no timestamps")
    else:
      # nothing to do
      sys.exit(0)

  if len(zip_ts_candidates) > 1:
    sys.exit(
      "Multiple timestamps, needs manual cleanup: {}".format(
        repr(zip_ts_candidates)))

  return zip_ts_candidates.pop()


if len(sys.argv) < 3:
  sys.exit(
    """usage: %s src dst
    src: a folder that recieves Takout zip files
    dst: the destination to unpack into""" % sys.argv[0])

takeout_src = sys.argv[1]
takeout_dst_root = sys.argv[2]

start = time()

src_files = [os.path.join(takeout_src, i) for i in os.listdir(takeout_src)]

dest_ts = validate_and_get_timestamp(src_files)

dest_dir = os.path.join(takeout_dst_root, dest_ts)

print("Dest dir: {}".format(dest_dir))
os.makedirs(dest_dir, exist_ok=True)

all_files = []
total_size = 0

for fname in src_files:
  if fname.endswith(".zip"):
    with zipfile.ZipFile(fname) as zip:
      files = zip.namelist()
      size = sum(zip.getinfo(z).file_size for z in files)
      all_files.extend(files)
      total_size += size
      zip.extractall(path=dest_dir)
    print("Extracted {} files, {} bytes from {}".format(len(files), size, fname))
  else:
    print("Copying {}".format(fname))
    all_files.append(fname)
    total_size += os.path.getsize(fname)
    shutil.copy(fname, dest_dir)

# only delete if we've successfully copied everything
# most of the time this isn't necessary, but it seems safer
for fname in src_files:
  #os.unlink(fname)
  pass

elapsed = time() - start

print("Complete: {} files, {} bytes in {} seconds".format(
    len(all_files), total_size, elapsed))