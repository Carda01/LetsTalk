import subprocess
import sys


print("Starting TBox Creation...")
subprocess.run(["python", "scripts/tbox.py"])

print("------------------------------------\n")
print("Starting ABox Creation")
subprocess.run(["python", "scripts/abox.py",sys.argv[1] ])
