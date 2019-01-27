import os
import struct
from sys import argv


class InputBuffer:
    def __init__(self, buf):
        self.buffer = buf


# execute result
EXECUTE_SUCCESS         = 0
EXECUTE_DUPLICATE_KEY   = 1
EXECUTE_TABLE_FULL      = 2

# meta command result
META_COMMAND_SUCCESS                = 0
META_COMMAND_UNRECOGNIZED_COMMAND   = 1

# prepare result
PREPARE_SUCCESS                 = 0
PREPARE_SYNTAX_ERROR            = 1
PREPARE_UNRECOGNIZED_SUCCESS    = 2
PREPARE_STRING_TOO_LONG         = 3
PREPARE_NEGATIVE_ID             = 4

# statement type
STATEMENT_INSERT    = 0
STATEMENT_SELECT    = 1

COLUMN_USERNAME_SIZE    = 32
COLUMN_EMAIL_SIZE       = 255


class Row:
    def __init__(self, id, username, email):
        self.id = id
        self.username = username
        self.email = email


class Statement:
    def __init__(self, _type):
        self.type = _type
        self.row_to_insert = None


# compact representation of a row
ID_SIZE         = struct.calcsize("I")
USERNAME_SIZE   = struct.calcsize("%ds" % COLUMN_USERNAME_SIZE)
EMAIL_SIZE      = struct.calcsize("%ds" % COLUMN_EMAIL_SIZE)
ID_OFFSET       = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE       = 4096
TABLE_MAX_PAGES = 100

malloc_a_page_memory = lambda: "\x00" * PAGE_SIZE
modify_memory = lambda page, buf, offset: page[:offset] + buf + page[(offset + len(buf)):]


class Pager:
    def __init__(self, file_descriptor, file_length, num_pages):
        self.file_descriptor = file_descriptor
        self.file_length = file_length
        self.num_pages = num_pages
        self.pages = ["" for _ in range(TABLE_MAX_PAGES)]


class Table:
    def __init__(self, pager, root_page_num):
        self.pager = pager
        self.root_page_num = root_page_num


class Cursor:
    def __init__(self, table, page_num, cell_num, end_of_table):
        self.table = table
        self.page_num = page_num
        self.cell_num = cell_num
        self.end_of_table = end_of_table


def print_row(row):
    id = row.id
    username = row.username.replace("\x00", "")
    email = row.email.replace("\x00", "")
    print "(%d, %s, %s)" % (id, username, email)


# node type
NODE_INTERNAL   = 0
NODE_LEAF       = 1

# common node header layout
NODE_TYPE_SIZE          = struct.calcsize("B")
NODE_TYPE_OFFSET        = 0
IS_ROOT_SIZE            = struct.calcsize("?")
IS_ROOT_OFFSET          = NODE_TYPE_SIZE
PARENT_POINTER_SIZE     = struct.calcsize("I")
PARENT_POINTER_OFFSET   = IS_ROOT_OFFSET + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

# internal node header layout
INTERNAL_NODE_NUM_KEYS_SIZE     = struct.calcsize("I")
INTERNAL_NODE_NUM_KEYS_OFFSET   = COMMON_NODE_HEADER_SIZE
INTERNAL_NODE_RIGHT_CHILD_SIZE  = struct.calcsize("I")
INTERNAL_NODE_RIGHT_CHILD_OFFSET= INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
INTERNAL_NODE_HEADER_SIZE       = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + \
                                    INTERNAL_NODE_RIGHT_CHILD_SIZE

# internal node body layout
INTERNAL_NODE_KEY_SIZE      = struct.calcsize("I")
INTERNAL_NODE_CHILD_SIZE    = struct.calcsize("I")
INTERNAL_NODE_CELL_SIZE     = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE

# leaf node header layout
LEAF_NODE_NUM_CELLS_SIZE    = struct.calcsize("I")
LEAF_NODE_NUM_CELLS_OFFSET  = COMMON_NODE_HEADER_SIZE
LEAF_NODE_HEADER_SIZE       = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE

# leaf node body layout
LEAF_NODE_KEY_SIZE          = struct.calcsize("I")
LEAF_NODE_KEY_OFFSET        = 0
LEAF_NODE_VALUE_SIZE        = ROW_SIZE
LEAF_NODE_VALUE_OFFSET      = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE         = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS   = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
LEAF_NODE_MAX_CELLS         = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2
LEAF_NODE_LEFT_SPLIT_COUNT  = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT


def get_node_type(node):
    return struct.unpack("B", node[NODE_TYPE_OFFSET])[0]


def set_node_type(node, type):
    buf = struct.pack("B", type)
    return modify_memory(node, buf, NODE_TYPE_OFFSET)


def is_node_root(node):
    offset = IS_ROOT_OFFSET
    string = node[offset: offset + IS_ROOT_SIZE]
    return struct.unpack("?", string)[0]


def set_node_root(node, is_root):
    buf = struct.pack("?", is_root)
    return modify_memory(node, buf, IS_ROOT_OFFSET)


def internal_node_num_keys(node):
    offset = INTERNAL_NODE_NUM_KEYS_OFFSET
    string = node[offset: offset + INTERNAL_NODE_NUM_KEYS_SIZE]
    return struct.unpack("I", string)[0]


def internal_node_right_child(node):
    offset = INTERNAL_NODE_RIGHT_CHILD_OFFSET
    string = node[offset: offset + INTERNAL_NODE_RIGHT_CHILD_SIZE]
    return struct.unpack("I", string)[0]


def internal_node_cell(node, cell_num):
    offset = INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE
    string = node[offset: offset + INTERNAL_NODE_CELL_SIZE]
    return struct.unpack("I", string)[0]


def internal_node_child(node, child_num):
    num_keys = internal_node_num_keys(node)
    if child_num > num_keys:
        print "Tried to access child_num %d > num_keys %d" % (child_num, num_keys)
        exit(0)
    elif child_num == num_keys:
        return internal_node_right_child(node)
    else:
        offset = INTERNAL_NODE_HEADER_SIZE + child_num * INTERNAL_NODE_CELL_SIZE
        string = node[offset: offset + INTERNAL_NODE_CHILD_SIZE]
        return struct.unpack("I", string)[0]


def internal_node_key(node, key_num):
    offset = INTERNAL_NODE_HEADER_SIZE + key_num * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE
    string = node[offset: offset + INTERNAL_NODE_KEY_SIZE]
    return struct.unpack("I", string)[0]


def leaf_node_num_cells(node):
    offset = LEAF_NODE_NUM_CELLS_OFFSET
    string = node[offset: offset + LEAF_NODE_NUM_CELLS_SIZE]
    return struct.unpack("I", string)[0]


def leaf_node_cell(node, cell_num):
    offset = LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE
    return node[offset: offset + LEAF_NODE_CELL_SIZE]


def leaf_node_key(node, cell_num):
    offset = LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE
    string = node[offset: offset + LEAF_NODE_KEY_SIZE]
    return struct.unpack("I", string)[0]


def leaf_node_value(node, cell_num):
    offset = (LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE) + LEAF_NODE_KEY_SIZE
    return node[offset: offset + LEAF_NODE_VALUE_SIZE]


def get_node_max_key(node):
    result = get_node_type(node)
    if result == NODE_INTERNAL:
        return internal_node_key(node, internal_node_num_keys(node) - 1)
    elif result == NODE_LEAF:
        return leaf_node_key(node, leaf_node_num_cells(node) - 1)


def print_constants():
    print "ROW_SIZE: %d" % ROW_SIZE
    print "COMMON_NODE_HEADER_SIZE: %d" % COMMON_NODE_HEADER_SIZE
    print "LEAF_NODE_HEADER_SIZE: %d" % LEAF_NODE_HEADER_SIZE
    print "LEAF_NODE_CELL_SIZE: %d" % LEAF_NODE_CELL_SIZE
    print "LEAF_NODE_SPACE_FOR_CELLS: %d" % LEAF_NODE_SPACE_FOR_CELLS
    print "LEAF_NODE_MAX_CELLS: %d" % LEAF_NODE_MAX_CELLS


def indent(level):
    for i in range(level):
        print " ",


def print_tree(pager, page_num, indentation_level):
    node = get_page(pager, page_num)
    result = get_node_type(node)

    if result == NODE_LEAF:
        num_keys = leaf_node_num_cells(node)
        indent(indentation_level)
        print "- leaf (size %d)" % num_keys
        for i in range(num_keys):
            indent(indentation_level + 1)
            print "- %d" % leaf_node_key(node, i)
    elif result == NODE_INTERNAL:
        num_keys = internal_node_num_keys(node)
        indent(indentation_level)
        print "- internal (size %d)" % num_keys
        for i in range(num_keys):
            child = internal_node_child(node, i)
            print_tree(pager, child, indentation_level + 1)

            indent(indentation_level)
            print "- key %d" % internal_node_key(node, i)
        child = internal_node_right_child(node)
        print_tree(pager, child, indentation_level + 1)


def serialize_row(src):
    dest = struct.pack("I", src.id)
    dest = dest + struct.pack("%ds" % USERNAME_SIZE, src.username)
    dest = dest + struct.pack("%ds" % EMAIL_SIZE, src.email)
    return dest


def deserialize_row(src):
    id = struct.unpack("I", src[ID_OFFSET: ID_OFFSET + ID_SIZE])[0]
    username = struct.unpack("%ds" % USERNAME_SIZE, src[USERNAME_OFFSET: USERNAME_OFFSET + USERNAME_SIZE])[0]
    email = struct.unpack("%ds" % EMAIL_SIZE, src[EMAIL_OFFSET: EMAIL_OFFSET + EMAIL_SIZE])[0]
    return Row(id, username, email)


def initialize_leaf_node(node):
    # initialize node type
    node = set_node_type(node, NODE_LEAF)
    # initialize node root
    node = set_node_root(node, False)
    # initialize num cells
    buf = struct.pack("I", 0)
    node = modify_memory(node, buf, LEAF_NODE_NUM_CELLS_OFFSET)
    return node


def initialize_internal_node(node):
    # initialize node type
    node = set_node_type(node, NODE_INTERNAL)
    # initialize node root
    node = set_node_root(node, False)
    # initialize num keys
    buf = struct.pack("I", 0)
    node = modify_memory(node, buf, INTERNAL_NODE_NUM_KEYS_OFFSET)
    return node


def leaf_node_find(table, page_num, key):
    node = get_page(table.pager, page_num)
    num_cells = leaf_node_num_cells(node)

    cursor = Cursor(table, page_num, None, None)

    # Binary search
    min_index = 0
    one_past_max_index = num_cells
    while one_past_max_index != min_index:
        index = (min_index + one_past_max_index) / 2
        key_at_index = leaf_node_key(node, index)
        if key == key_at_index:
            cursor.cell_num = index
            return cursor
        if key < key_at_index:
            one_past_max_index = index
        else:
            min_index = index + 1

    cursor.cell_num = min_index
    return cursor


def get_page(pager, page_num):
    if page_num > TABLE_MAX_PAGES:
        print "Tried to fetch page number out of bounds. %d > %d" % (page_num, TABLE_MAX_PAGES)
        exit(0)

    if not pager.pages[page_num]:
        page = malloc_a_page_memory()
        num_pages = pager.file_length / PAGE_SIZE
        if pager.file_length % PAGE_SIZE:
            num_pages += 1
        if page_num <= num_pages:
            pager.file_descriptor.seek(page_num * PAGE_SIZE, os.SEEK_SET)
            buf = pager.file_descriptor.read(PAGE_SIZE)
            if buf:
                page = buf
        pager.pages[page_num] = page
        if page_num >= pager.num_pages:
            pager.num_pages = page_num + 1

    return pager.pages[page_num]


# Return the position of the given key
# if key is not present, return the position where it should be inserted
def table_find(table, key):
    root_page_num = table.root_page_num
    root_node = get_page(table.pager, root_page_num)

    if get_node_type(root_node) == NODE_LEAF:
        return leaf_node_find(table, root_page_num, key)
    else:
        print "Need to implement searching an internal node"
        exit(0)


def table_start(table):
    root_node = get_page(table.pager, table.root_page_num)
    num_cells = leaf_node_num_cells(root_node)
    return Cursor(table, table.root_page_num, 0, num_cells == 0)


def cursor_value(cursor):
    page_num = cursor.page_num
    page = get_page(cursor.table.pager, page_num)
    return leaf_node_value(page, cursor.cell_num)


def cursor_advance(cursor):
    page_num = cursor.page_num
    node = get_page(cursor.table.pager, page_num)

    cursor.cell_num += 1
    if cursor.cell_num >= leaf_node_num_cells(node):
        cursor.end_of_table = True


def pager_open(filename):
    fd = open(filename, "rb+")
    fd.seek(0, os.SEEK_END)
    file_length = fd.tell()
    if file_length % PAGE_SIZE != 0:
        print "Db file is not a whole number of pages. Corrupt file."
        exit(0)
    num_pages = file_length / PAGE_SIZE

    pager = Pager(fd, file_length, num_pages)
    return pager


def db_open(filename):
    pager = pager_open(filename)
    table = Table(pager, 0)

    if pager.num_pages == 0:
        # New database file. Initialize page 0 as leaf node.
        root_node = get_page(pager, 0)
        root_node = initialize_leaf_node(root_node)
        root_node = set_node_root(root_node, True)
        table.pager.pages[0] = root_node

    return table


def print_prompt():
    print "db >",


def read_input():
    buf = raw_input()
    return InputBuffer(buf)


def pager_flush(pager, page_num):
    if not pager.pages[page_num]:
        print "Tried to flush null page"
        exit(0)

    pager.file_descriptor.seek(page_num * PAGE_SIZE, os.SEEK_SET)
    pager.file_descriptor.write(pager.pages[page_num])


def db_close(table):
    pager = table.pager

    for i in range(pager.num_pages):
        if not pager.pages[i]:
            continue
        pager_flush(pager, i)

    pager.file_descriptor.close()


def do_meta_command(input_buffer, table):
    if input_buffer.buffer == ".exit":
        db_close(table)
        exit(1)
    elif input_buffer.buffer == ".btree":
        print "Tree:"
        print_tree(table.pager, 0, 0)
        return META_COMMAND_SUCCESS
    elif input_buffer.buffer == ".constants":
        print "Constants:"
        print_constants()
        return META_COMMAND_SUCCESS
    else:
        return META_COMMAND_UNRECOGNIZED_COMMAND


def prepare_insert(input_buffer):
    args = input_buffer.buffer.split(" ", 3)
    if len(args) < 4:
        return None, PREPARE_SYNTAX_ERROR
    id = int(args[1])
    username = args[2]
    email = args[3]

    statement = Statement(STATEMENT_INSERT)
    if id < 0:
        return statement, PREPARE_NEGATIVE_ID
    if len(username) > COLUMN_USERNAME_SIZE:
        return statement, PREPARE_STRING_TOO_LONG
    if len(email) > COLUMN_EMAIL_SIZE:
        return statement, PREPARE_STRING_TOO_LONG

    statement.row_to_insert = Row(id, username, email)
    return statement, PREPARE_SUCCESS


def prepare_statement(input_buffer):
    if input_buffer.buffer[:6] == "insert":
        return prepare_insert(input_buffer)
    elif input_buffer.buffer == "select":
        return Statement(STATEMENT_SELECT), PREPARE_SUCCESS
    else:
        return None, PREPARE_UNRECOGNIZED_SUCCESS


# until recycling free pages, new pages will always go onto the end of the database file
def get_unused_page_num(pager):
    return pager.num_pages


def create_new_root(table, right_child_page_num):
    # handle splitting the root
    # old root copied to new page, becomes left child
    # address of right child passed in
    # re-initialize root page to contain the new root node
    # new root node points to two children
    root = get_page(table.pager, table.root_page_num)
    right_child = get_page(table.pager, right_child_page_num)
    left_child_page_num = get_unused_page_num(table.pager)
    left_child = get_page(table.pager, left_child_page_num)

    # left child has data copied from old root
    left_child = root
    left_child = set_node_root(left_child, False)

    # root node is a new internal node with one key and two children
    root = initialize_internal_node(root)
    root = set_node_root(root, True)
    # set internal node num key
    buf = struct.pack("I", 1)
    root = modify_memory(root, buf, INTERNAL_NODE_NUM_KEYS_OFFSET)
    # set internal node child
    buf = struct.pack("I", left_child_page_num)
    num_keys = internal_node_num_keys(root)
    if num_keys < 0:
        print "Tried to access child_num %d > num_keys %d" % (0, num_keys)
        exit(0)
    elif num_keys == 0:
        root = modify_memory(root, buf, INTERNAL_NODE_RIGHT_CHILD_OFFSET)
    else:
        root = modify_memory(root, buf, INTERNAL_NODE_HEADER_SIZE)
    # set internal node key
    left_child_max_key = get_node_max_key(left_child)
    buf = struct.pack("I", left_child_max_key)
    root = modify_memory(root, buf, INTERNAL_NODE_HEADER_SIZE + INTERNAL_NODE_CHILD_SIZE)
    # set internal node right child
    buf = struct.pack("I", right_child_page_num)
    root = modify_memory(root, buf, INTERNAL_NODE_RIGHT_CHILD_OFFSET)

    table.pager.pages[table.root_page_num] = root
    table.pager.pages[right_child_page_num] = right_child
    table.pager.pages[left_child_page_num] = left_child


def leaf_node_split_and_insert(cursor, key, value):
    # create a new node move half the cells over
    # insert the new value in one of the two nodes
    # update parent or create a new parent
    old_node = get_page(cursor.table.pager, cursor.page_num)
    new_page_num = get_unused_page_num(cursor.table.pager)
    new_node = get_page(cursor.table.pager, new_page_num)
    new_node = initialize_leaf_node(new_node)

    # all existing keys plus new key should be divided evenly between old and new nodes
    # starting from the right, move each key to correct position
    for i in range(LEAF_NODE_MAX_CELLS, 0, -1):
        if i == cursor.cell_num:
            buf = serialize_row(value)
        elif i > cursor.cell_num:
            pos = LEAF_NODE_HEADER_SIZE + (i - 1) * LEAF_NODE_CELL_SIZE
            buf = old_node[pos: pos + LEAF_NODE_CELL_SIZE]
        else:
            pos = LEAF_NODE_HEADER_SIZE + i * LEAF_NODE_CELL_SIZE
            buf = old_node[pos: pos + LEAF_NODE_CELL_SIZE]

        index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT
        offset = LEAF_NODE_HEADER_SIZE + index_within_node * LEAF_NODE_CELL_SIZE

        if i >= LEAF_NODE_LEFT_SPLIT_COUNT:
            new_node = modify_memory(new_node, buf, offset)
        else:
            old_node = modify_memory(old_node, buf, offset)

    # update cell count on both leaf nodes
    buf = struct.pack("I", LEAF_NODE_LEFT_SPLIT_COUNT)
    old_node = modify_memory(old_node, buf, LEAF_NODE_NUM_CELLS_OFFSET)
    buf = struct.pack("I", LEAF_NODE_RIGHT_SPLIT_COUNT)
    new_node = modify_memory(new_node, buf, LEAF_NODE_NUM_CELLS_OFFSET)

    cursor.table.pager.pages[cursor.page_num] = old_node
    cursor.table.pager.pages[new_page_num] = new_node

    if is_node_root(old_node):
        create_new_root(cursor.table, new_page_num)
        return
    else:
        print "Need to implement updating parent after split"
        exit(0)



def leaf_node_insert(cursor, key, value):
    node = get_page(cursor.table.pager, cursor.page_num)

    num_cells = leaf_node_num_cells(node)
    if num_cells >= LEAF_NODE_MAX_CELLS:
        # node full
        leaf_node_split_and_insert(cursor, key, value)
        return

    if cursor.cell_num < num_cells:
        # Make room for new cell
        for i in range(num_cells, cursor.cell_num, -1):
            offset = LEAF_NODE_HEADER_SIZE + i * LEAF_NODE_CELL_SIZE
            node = modify_memory(node, leaf_node_cell(node, i - 1), offset)

    # update num cells
    buf = struct.pack("I", leaf_node_num_cells(node) + 1)
    node = modify_memory(node, buf, LEAF_NODE_NUM_CELLS_OFFSET)
    # insert node key
    buf = struct.pack("I", key)
    offset = LEAF_NODE_HEADER_SIZE + cursor.cell_num * LEAF_NODE_CELL_SIZE
    node = modify_memory(node, buf, offset)
    # insert node value
    buf = serialize_row(value)
    offset = (LEAF_NODE_HEADER_SIZE + cursor.cell_num * LEAF_NODE_CELL_SIZE) + LEAF_NODE_KEY_SIZE
    node = modify_memory(node, buf, offset)

    cursor.table.pager.pages[cursor.page_num] = node


def execute_insert(statement, table):
    node = get_page(table.pager, table.root_page_num)
    num_cells = leaf_node_num_cells(node)

    row_to_insert = statement.row_to_insert
    key_to_insert = row_to_insert.id
    cursor = table_find(table, key_to_insert)

    if cursor.cell_num < num_cells:
        key_at_index = leaf_node_key(node, cursor.cell_num)
        if key_at_index == key_to_insert:
            return EXECUTE_DUPLICATE_KEY

    leaf_node_insert(cursor, row_to_insert.id, row_to_insert)
    return EXECUTE_SUCCESS


def execute_select(table):
    cursor = table_start(table)
    while not cursor.end_of_table:
        row = deserialize_row(cursor_value(cursor))
        print_row(row)
        cursor_advance(cursor)

    return EXECUTE_SUCCESS


def execute_statement(statement, table):
    if statement.type == STATEMENT_INSERT:
        return execute_insert(statement, table)
    elif statement.type == STATEMENT_SELECT:
        return execute_select(table)


def main(argv):
    # if len(argv) < 2:
    #     print "Must supply a database filename."
    #     exit(0)
    # filename = argv[1]
    filename = "mydb.db"
    table = db_open(filename)
    while True:
        print_prompt()
        input_buffer = read_input()

        if input_buffer.buffer[0] == ".":
            result = do_meta_command(input_buffer, table)
            if result == META_COMMAND_SUCCESS:
                continue
            elif result == META_COMMAND_UNRECOGNIZED_COMMAND:
                print "Unrecognized command '%s'." % input_buffer.buffer
                continue

        statement, result = prepare_statement(input_buffer)
        if result == PREPARE_SUCCESS:
            pass
        elif result == PREPARE_NEGATIVE_ID:
            print "ID must be positive."
            continue
        elif result == PREPARE_STRING_TOO_LONG:
            print "String is too long."
            continue
        elif result == PREPARE_SYNTAX_ERROR:
            print "Syntax error. Could not parse statement."
            continue
        elif result == PREPARE_UNRECOGNIZED_SUCCESS:
            print "Unrecognized keyword at start of '%s'." % input_buffer.buffer
            continue

        result = execute_statement(statement, table)
        if result == EXECUTE_SUCCESS:
            print "Executed."
        elif result == EXECUTE_DUPLICATE_KEY:
            print "Error: Duplicate key."
        elif result == EXECUTE_TABLE_FULL:
            print "Error: Table full."


if __name__ == "__main__":
    main(argv)

