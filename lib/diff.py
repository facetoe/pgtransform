class DiffItem(object):
    def __init__(self, name, expected, found):
        self.name = name
        self.expected = expected
        self.found = found

    def __str__(self):
        return "[%s - expected: %s, found: %s]" % (self.name, self.expected, self.found)

    def __repr__(self):
        return "{%s}" % self.name


class DiffNode(object):
    """
    Node representing a database difference.
    """

    def __init__(self, name, object_type=None, data=None, parent=None):
        self.parent = parent
        self.name = name
        self.data = data
        self.object_type = object_type
        self.children = []

    def append(self, obj):
        if isinstance(obj, DiffNode):
            obj.parent = self
        self.children.append(obj)

    def isleaf(self):
        return len(self.children) == 0

    def isbranch(self):
        return len(self.children) > 0

    def find(self, target, attribute='name'):
        if getattr(self, attribute) == target:
            return self
        return self._find(target, attribute, node=self)

    def _find(self, target, attribute, node):
        for n in node.children:
            if getattr(n, attribute) == target:
                return n
            found = n.find(target=target, attribute=attribute, node=n)
            if found:
                return found

    def findall(self, target, attribute='name'):
        values = []
        self._findall(self, attribute, target, values)
        return values

    def _findall(self, node, attribute, target, values):
        if node is not None:
            if getattr(node, attribute) == target:
                values.append(node)
            for n in node.children:
                self._findall(n, attribute, target, values)

    # This only merges top level nodes. Need to fix it.
    def merge(self, node):
        if self.name == node.name:
            for child in node.children:
                self.append(child)

    def to_tree(self, level=0):
        indent_width = '    '
        indent = indent_width * level
        output = "%s%s:\n" % (indent, self.name)
        for child in self.children:
            if child.isleaf():
                output += "%s -> %s: " \
                          "expected: %s, " \
                          "found: %s\n" % ((indent + indent_width),
                                           repr(child.data),
                                           repr(child.data.expected),
                                           repr(child.data.found))
            else:
                output += child.to_tree(level + 1)
        return output

    def __len__(self):
        return len(self.children)

    def __repr__(self):
        return "{%s}" % self.name
