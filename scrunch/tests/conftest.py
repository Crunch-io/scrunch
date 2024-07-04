import sys

import pytest

mark_fail_py2 = pytest.mark.xfail(sys.version_info < (3, 0,), reason="py2 order in args causes tests failures")
