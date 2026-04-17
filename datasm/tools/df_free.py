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

# print(f"DIR = {indir}:")
# print(f"    Total: {totTB:>20} TB")
# print(f"     Used: {useTB:>20} TB")
print(f"     Free: {freTB:>20} TB")
