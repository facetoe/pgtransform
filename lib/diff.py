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
    def __init__(self, name, object_type=None, expected_obj=None, data=None, parent=None):
        self.parent = parent
        self.name = name
        self.data = data
        self.object_type = object_type
        self.expected_obj = expected_obj
        self.children = []

    def append(self, obj):
        if isinstance(obj, DiffNode):
            obj.parent = self
        self.children.append(obj)

    def isleaf(self):
        return len(self.children) == 0

    def isbranch(self):
        return len(self.children) > 0

    def walk(self):
        return self.__walk(self)

    def __walk(self, node):
        yield node
        for child in node.children:
            for n in self.__walk(child):
                yield n

    def find(self, target, attribute='name'):
        if getattr(self, attribute) == target:
            return self
        return self.__find(target, attribute, node=self)

    def __find(self, target, attribute, node):
        for n in node.children:
            if getattr(n, attribute) == target:
                return n
            found = n.__find(target=target, attribute=attribute, node=n)
            if found:
                return found

    def findall(self, target, attribute='name'):
        values = []
        self.__findall(self, attribute, target, values)
        return values

    def __findall(self, node, attribute, target, values):
        if node is not None:
            if getattr(node, attribute) == target:
                values.append(node)
            for n in node.children:
                self.__findall(n, attribute, target, values)

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
