# Ensure that tests are importing the local copy of txredisapi rather than
# any system-installed copy of txredisapi that might exist in the path.
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

import txredisapi

sys.path.pop(0)
