import os
import struct
from sys import argv


class InputBuffer:
    def __init__(self, buf):
        self.buffer = buf


# execute result
EXECUTE_SUCCESS = 0
EXECUTE_TABLE_FULL = 1

# meta command result
META_COMMAND_SUCCESS = 0
META_COMMAND_UNRECOGNIZED_COMMAND = 1

# prepare result
PREPARE_SUCCESS = 0
PREPARE_SYNTAX_ERROR = 1
PREPARE_UNRECOGNIZED_SUCCESS = 2
PREPARE_STRING_TOO_LONG = 3
PREPARE_NEGATIVE_ID = 4

# statement type
STATEMENT_INSERT = 0
STATEMENT_SELECT = 1

COLUMN_USERNAME_SIZE = 32
COLUMN_EMAIL_SIZE = 255


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
ID_SIZE = struct.calcsize("I")
USERNAME_SIZE = struct.calcsize("%ds" % COLUMN_USERNAME_SIZE)
EMAIL_SIZE = struct.calcsize("%ds" % COLUMN_EMAIL_SIZE)
ID_OFFSET = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE = 4096
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
NODE_INTERNAL = 0
NODE_LEAF = 1

# common node header layout
NODE_TYPE_SIZE = struct.calcsize("B")
NODE_TYPE_OFFSET = 0
IS_ROOT_SIZE = struct.calcsize("B")
IS_ROOT_OFFSET = NODE_TYPE_SIZE
PARENT_POINTER_SIZE = struct.calcsize("I")
PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

# leaf node header layout
LEAF_NODE_NUM_CELLS_SIZE = struct.calcsize("I")
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE

# leaf node body layout
LEAF_NODE_KEY_SIZE = struct.calcsize("I")
LEAF_NODE_KEY_OFFSET = 0
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE


def leaf_node_num_cells(node):
    offset = LEAF_NODE_NUM_CELLS_OFFSET
    string = node[offset: offset + LEAF_NODE_NUM_CELLS_SIZE]
    return struct.unpack("I", string)[0]


def leaf_node_cell(node, cell_num):
    offset = LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE
    return node[offset: offset + LEAF_NODE_NUM_CELLS_SIZE]


def leaf_node_key(node, cell_num):
    buf = leaf_node_cell(node, cell_num)
    return struct.unpack("I", buf)[0]


def leaf_node_value(node, cell_num):
    offset = (LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE) + LEAF_NODE_KEY_SIZE
    return node[offset: offset + LEAF_NODE_VALUE_SIZE]


def print_constants():
    print "ROW_SIZE: %d" % ROW_SIZE
    print "COMMON_NODE_HEADER_SIZE: %d" % COMMON_NODE_HEADER_SIZE
    print "LEAF_NODE_HEADER_SIZE: %d" % LEAF_NODE_HEADER_SIZE
    print "LEAF_NODE_CELL_SIZE: %d" % LEAF_NODE_CELL_SIZE
    print "LEAF_NODE_SPACE_FOR_CELLS: %d" % LEAF_NODE_SPACE_FOR_CELLS
    print "LEAF_NODE_MAX_CELLS: %d" % LEAF_NODE_MAX_CELLS


def print_leaf_node(node):
    num_cells = leaf_node_num_cells(node)
    print "leaf (size %d)" % num_cells
    for i in range(num_cells):
        key = leaf_node_key(node, i)
        print "  - %d : %d" % (i, key)


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
    buf = struct.pack("I", 0)
    return modify_memory(node, buf, LEAF_NODE_NUM_CELLS_OFFSET)


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


def table_start(table):
    root_node = get_page(table.pager, table.root_page_num)
    num_cells = leaf_node_num_cells(root_node)
    return Cursor(table, table.root_page_num, 0, num_cells == 0)


def table_end(table):
    root_node = get_page(table.pager, table.root_page_num)
    num_cells = leaf_node_num_cells(root_node)
    return Cursor(table, table.root_page_num, num_cells, True)


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
        table.pager.pages[0] = initialize_leaf_node(root_node)

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
        print_leaf_node(get_page(table.pager, 0))
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


def leaf_node_insert(cursor, key, value):
    node = get_page(cursor.table.pager, cursor.page_num)

    num_cells = leaf_node_num_cells(node)
    if num_cells >= LEAF_NODE_MAX_CELLS:
        # node full
        print "Need to implement splitting a leaf node."
        exit(0)

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
    if leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS:
        return EXECUTE_TABLE_FULL

    row_to_insert = statement.row_to_insert
    cursor = table_end(table)

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
    if len(argv) < 2:
        print "Must supply a database filename."
        exit(0)
    filename = argv[1]
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
        elif result == EXECUTE_TABLE_FULL:
            print "Error: Table full."


if __name__ == "__main__":
    main(argv)

