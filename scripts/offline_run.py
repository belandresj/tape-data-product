"""Run the installed CLI with Python network and checkout-access audit guards.

Run from an empty directory outside every --forbid root, using a non-editable
installation. This is a reproducibility check, not an OS security sandbox.
"""
import argparse
import os
from pathlib import Path
import runpy
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--forbid', type=Path, action='append', required=True,
                        help='Reject reads beneath this checkout (repeatable)')
    parser.add_argument('arguments', nargs=argparse.REMAINDER,
                        help='CLI arguments after --')
    args = parser.parse_args()
    forbidden = tuple(path.resolve() for path in args.forbid)
    if any(Path.cwd().is_relative_to(root) for root in forbidden):
        parser.error('Run from a directory outside the forbidden roots')

    def audit(event, values):
        if event in ('open', 'os.listdir', 'os.scandir') and values:
            value = values[0]
            if isinstance(value, (str, bytes, os.PathLike)):
                path = Path(os.fsdecode(value)).resolve()
                if any(path.is_relative_to(root) for root in forbidden):
                    raise RuntimeError('Offline run attempted checkout access: ' + str(path))
        if event in ('socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'):
            raise RuntimeError('Offline run attempted network or subprocess access')

    sys.addaudithook(audit)
    arguments = args.arguments
    if arguments[:1] == ['--']:
        arguments = arguments[1:]
    sys.argv = ['tape-product', *arguments]
    runpy.run_module('tape_data_product.cli', run_name='__main__')


if __name__ == '__main__':
    main()
