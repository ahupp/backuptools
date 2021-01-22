"""

Google Takeout (https://takeout.google.com/) is a tool to export all of your
Google data (photos, mail, comments, etc).  It can save the export into your
Google Drive, and can be scheduled to run every 2 months for a year.

This script has two purposes:

1. Unzip the exported zip files from Google Drive into a non-Drive location, so
   that an incremental backup can pickup just the differences.  The source files
   can then be deleted from Google Drive to free up space.
2. Report an error if its been more than 2 months since the last export.  This
   ensures I won't forget to re-start the export process after it expires.

Usage:
python3 ./takeout_export.py Google\ Drive/Takeout /some/destination/folder

"""


import os
import sys
import zipfile
import re
import shutil
import zipfile
from time import time
from datetime import datetime, timedelta

if len(sys.argv) < 3:
  sys.exit(
    """usage: %s src dst
    src: a folder that recieves Takeout zip files, like Google Drive
    dst: the destination to unpack into""" % sys.argv[0])

takeout_src = sys.argv[1]
takeout_dst_root = sys.argv[2]

def dest_dir_timestamps():
  # return [(parsed timestamp, dir name), ...]
  ret = []
  ts_re = re.compile("^([0-9T]+)T")
  for d in os.listdir(takeout_dst_root):
    m = ts_re.match(d)
    if m:
      ts_str = m.group(1)
      ts = datetime.strptime(ts_str, "%Y%m%d")
      ret.append((ts, os.path.join(takeout_dst_root, d)))
  return ret

def validate_time_since_last_export():
  dts = dest_dir_timestamps()
  if not dts:
    return
  dts.sort(key=lambda i: i[0])
  (most_recent_ts, most_recent_dir) = dts.pop()
  elapsed = datetime.now() - most_recent_ts
  limit = timedelta(weeks=9)
  if elapsed > limit:
    sys.exit("More than 2 months elapsed since last export, last is {}".format(most_recent_dir))


def validate_and_get_timestamp(src_files):
  zip_ts_candidates = set()
  part_num = []

  # foo/bar/takeout-20210122T074607Z-001.zi -> 20210122T074607Z
  ts_re = re.compile(".*/takeout-([0-9TZ]*)-\d+\.zip$")
  # foo/bar/something-123.mp4 -> 123
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
      validate_time_since_last_export()
      # if we get here, nothing to do
      sys.exit(0)

  if len(zip_ts_candidates) > 1:
    sys.exit(
      "Multiple timestamps, needs manual cleanup: {}".format(
        repr(zip_ts_candidates)))

  return zip_ts_candidates.pop()

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
    # could be move, but makes testing easier
    shutil.copy(fname, dest_dir)

# only delete if we've successfully copied everything most of the time this
# isn't necessary, but it seems safer
for fname in src_files:
  os.unlink(fname)

elapsed = time() - start

print("Complete: {} files, {} bytes in {} seconds".format(
    len(all_files), total_size, elapsed))
