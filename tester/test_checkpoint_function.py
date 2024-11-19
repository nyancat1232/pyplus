import unittest

from builtin.checkpoint_function import CheckPointFunctionDecoration

class TestCheckPointFunction(unittest.TestCase):
    def test_checkpoint_function(self):
        @CheckPointFunctionDecoration
        def test(val:int):
            receiver = yield val,'first'
            yield receiver+val,'second'

        self.assertEqual(test(3).second(first=-2), 1)


if __name__ == '__main__':
    unittest.main()