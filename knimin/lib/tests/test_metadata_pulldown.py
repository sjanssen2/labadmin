from unittest import TestCase, main

from knimin.lib.metadata_pulldown import (gather_agsurveys)


class TestMetadataPulldown(TestCase):
    def test_gather_agsurveys(self):
        gather_agsurveys()


if __name__ == '__main__':
    main()
