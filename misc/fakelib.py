from __future__ import print_function
import os
import re
import subprocess
import sys

import distutils.msvccompiler

# Building py-lmdb 
import pefile


# http://stackoverflow.com/questions/6774967/lnk2019-unresolved-external-symbol-ntopenfile


def get_lib_exe():
    compiler = distutils.msvccompiler.MSVCCompiler()
    compiler.initialize()
    return compiler.lib


def main(dll_path):
    dll_path = os.path.abspath(dll_path)

    print("Parsing %s" % (dll_path,))

    pe = pefile.PE(dll_path)
    if not getattr(pe, "DIRECTORY_ENTRY_EXPORT", None):
        print("ERROR: given file has no exports.")
        return 1

    is_64bit = sys.maxsize > (2**32)
    if is_64bit:
        machine = 'x64'
    else:
        machine = 'x86'

    dll_filename = os.path.basename(dll_path)
    lib_filename = re.sub(
        r"(?i)^.*?([^\\/]+)\.(?:dll|exe|sys|ocx)$",
        r"\1.lib",
        dll_filename)
    def_filename = lib_filename.replace(".lib", ".def")
    def_path = os.path.join(os.getcwd(), def_filename)
    lib_path = os.path.join(os.getcwd(), lib_filename)

    print("Writing module definition file %s for %s" %
          (def_path, dll_path))

    with open(def_path, "w") as f:
        f.write("LIBRARY %s\n\n" % (dll_filename,))
        f.write("EXPORTS\n")
        numexp = 0
        for symbol in pe.DIRECTORY_ENTRY_EXPORT.symbols:
            if symbol.name:
                numexp += 1
                #f.write("\t%s\n" % (symbol.name.decode(),))

    print("Wrote %s with %d exports" % (def_path, numexp))
    args = [
        get_lib_exe(),
        '/machine:' + machine,
        '/def:' + def_path,
        '/out:' + lib_path
    ]

    print("Running %s" % (args,))
    subprocess.check_call(args)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit("ERROR:\n\tSyntax: fakelib <dllfile>\n")
    sys.exit(main(sys.argv[1]))
