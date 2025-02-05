CONFIG = dict((('extra_compile_args', ['-DHAVE_PATCHED_LMDB=1', '-UNDEBUG', '-w', '/FIPython.h']), ('extra_sources', ['build/lib/mdb.c', 'build/lib/midl.c']), ('extra_library_dirs', []), ('extra_include_dirs', ['lib/py-lmdb', 'build/lib', 'lib\\win32']), ('libraries', ['Advapi32', 'ntdll'])))

