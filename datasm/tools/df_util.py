import os
import sys
import shutil

if len(sys.argv) == 1:
    indir = os.getcwd()
if len(sys.argv) > 1:
    arg1 = sys.argv[1]
    if arg1 in ["help", "-h", "--help"]:
        print(f"Usage: python {sys.argv[0]} [directory]")
        print("If no directory is given, the current directory is applied")
        sys.exit(0)
    else:
        indir = arg1

total, used, free = shutil.disk_usage(indir)

totTB = int(total /1000000000000)
useTB = int(used /1000000000000)
freTB = int(free /1000000000000)

stats = os.statvfs(indir)
totInode = stats.f_files
freInode = stats.f_ffree
useInode = totInode - freInode

print(f"DIR = {indir}:")
print("              Space           Inodes")
print(f"    Total: {totTB:>8} TB  {totInode:>12}")
print(f"     Used: {useTB:>8} TB  {useInode:>12}")
print(f"     Free: {freTB:>8} TB  {freInode:>12}")

