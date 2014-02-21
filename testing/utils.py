# use this module to deal with testing on 2.6 vs 2.7
try:
    import unittest2 as unittest
except ImportError:
    import unittest
