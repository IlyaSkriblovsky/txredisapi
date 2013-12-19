# Ensure that tests are importing the local copy of txredisapi rather than
# any system-installed copy of txredisapi that might exist in the path.
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import txredisapi
sys.path.pop(0)