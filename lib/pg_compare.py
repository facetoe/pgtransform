class PGCompare(object):
    """
    PGCompare takes a reference database and a test database, compares them and returns the differences
    represented as a tree of DiffNodes.
    """

    def __init__(self, reference_provider, test_provider):
        self.reference_db = reference_provider.get_database()
        self.test_db = test_provider.get_database()

    def compare(self):
        return self.reference_db.compare_to(self.test_db)
