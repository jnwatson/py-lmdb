from __future__ import print_function
import os
import re
import subprocess
import sys

# Building py-lmdb 
import pefile


# http://stackoverflow.com/questions/6774967/lnk2019-unresolved-external-symbol-ntopenfile

def main(pename):
    print("Parsing %s" % (pename,))
    pe = pefile.PE(pename)
    if not getattr(pe, "DIRECTORY_ENTRY_EXPORT", None):
        print("ERROR: given file has no exports.")
        return 1

    is_64bit = sys.maxsize > (2**32)
    if is_64bit:
        machine = 'x64'
    else:
        machine = 'x86'

    modname = os.path.basename(pename)
    libname = re.sub(r"(?i)^.*?([^\\/]+)\.(?:dll|exe|sys|ocx)$", r"\1.lib", modname)
    defname = libname.replace(".lib", ".def")

    print("Writing module definition file %s for %s" % (defname, modname))
    with open(defname, "w") as f: # want it to throw, no sophisticated error handling here
        f.write("LIBRARY %s\n\n" % (modname,))
        f.write("EXPORTS\n")
        numexp = 0
        for exp in [x for x in pe.DIRECTORY_ENTRY_EXPORT.symbols if x.name]:
            numexp += 1
            f.write("\t%s\n" % (exp.name,))
    print("Wrote %s with %d exports" % (defname, numexp))
    args = ['lib', '/machine:' + machine, '/def:' + defname, '/out:' + libname]
    print("Running %s" % (args,))
    subprocess.check_call(args)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit("ERROR:\n\tSyntax: fakelib <dllfile>\n")
    sys.exit(main(sys.argv[1]))
