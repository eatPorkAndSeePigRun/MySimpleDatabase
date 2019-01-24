import os
import sys
import struct


class InputBuffer:
    def __init__(self, buffer=""):
        self.buffer = buffer


# execute result
EXECUTE_SUCCESS     = 0
EXECUTE_TABLE_FULL  = 1

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
    def __init__(self, id=0, username="", email=""):
        self.id = id
        self.username = username
        self.email = email


class Statement:
    def __init__(self, type=0, row_to_insert=Row()):
        self.type = type
        self.row_to_insert = row_to_insert


# compact representation of a row
ID_SIZE         = struct.calcsize("i")
USERNAME_SIZE   = struct.calcsize("%ds" % COLUMN_USERNAME_SIZE)
EMAIL_SIZE      = struct.calcsize("%ds" % COLUMN_EMAIL_SIZE)
ID_OFFSET       = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE       = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES


class Pager:
    def __init__(self, file_descriptor=-1, file_length=-1, pages=["" for i in range(TABLE_MAX_PAGES)]):
        self.file_descriptor = file_descriptor
        self.file_length = file_length
        self.pages = pages


class Table:
    def __init__(self, pager, num_rows):
        self.pager = pager
        self.num_rows = num_rows


def print_row(row):
    id = row.id
    username = row.username.replace("\x00", "")
    email = row.email.replace("\x00", "")
    print "(%d, %s, %s)" % (id, username, email)


def serialize_row(src, dest):
    temp = struct.pack("i", src.id)
    temp = temp + struct.pack("%ds" % USERNAME_SIZE, src.username)
    temp = temp + struct.pack("%ds" % EMAIL_SIZE, src.email)

    pages, page_num, byte_offset = dest
    old_page = pages[page_num]
    new_page = old_page[:byte_offset] + temp + old_page[byte_offset+len(temp):]
    pages[page_num] = new_page    


def deserialize_row(src, dest):
    pages, page_num, byte_offset = src
    page = pages[page_num]

    temp = page[byte_offset:]
    dest.id = struct.unpack("i", temp[ID_OFFSET : ID_OFFSET+ID_SIZE])[0]
    dest.username = struct.unpack("%ds" % USERNAME_SIZE, temp[USERNAME_OFFSET : USERNAME_OFFSET+USERNAME_SIZE])[0]
    dest.email = struct.unpack("%ds" % EMAIL_SIZE, temp[EMAIL_OFFSET : EMAIL_OFFSET+EMAIL_SIZE])[0]


def get_page(pager, page_num):
    malloc_memory = lambda : "\x00" * PAGE_SIZE

    if page_num > TABLE_MAX_PAGES:
        print "Tried to fetch page number out of bounds. %d > %d" % (page_num, TABLE_MAX_PAGES)
        exit(0)
    if not pager.pages[page_num]:
        page = malloc_memory()
        num_pages = pager.file_length / PAGE_SIZE
        if pager.file_length % PAGE_SIZE:
            num_pages += 1
        if page_num <= num_pages:
            pager.file_descriptor.seek(page_num * PAGE_SIZE, os.SEEK_SET)
            buf = pager.file_descriptor.read(PAGE_SIZE)
            if buf:
                page = buf + page[len(buf):]
        pager.pages[page_num] = page


def row_slot(table, row_num):
    page_num = row_num / ROWS_PER_PAGE
    get_page(table.pager, page_num)
    row_offset = row_num % ROWS_PER_PAGE
    byte_offset = row_offset * ROW_SIZE
    return table.pager.pages, page_num, byte_offset


def pager_open(filename):
    fd = open(filename, "rb+")
    fd.seek(0, os.SEEK_END)
    file_length = fd.tell()
    pager = Pager(fd, file_length)
    return pager 


def db_open(filename):
    pager = pager_open(filename)
    num_rows = pager.file_length / ROW_SIZE
    table = Table(pager, num_rows)
    return table


def print_prompt():
    print "db >",


def read_input():
    buf = raw_input()
    return InputBuffer(buf)


def pager_flush(pager, page_num, size):
    if not pager.pages[page_num]:
        print "Tried to flush null page"
        exit(0)

    pager.file_descriptor.seek(page_num * PAGE_SIZE, os.SEEK_SET)
    pager.file_descriptor.write(pager.pages[page_num][:size])


def db_close(table):
    #free_memory = lambda page: ""

    pager = table.pager
    num_full_pages = table.num_rows / ROWS_PER_PAGE

    for i in range(num_full_pages):
        if not pager.pages[i]:
            continue
        pager_flush(pager, i, PAGE_SIZE)
        #free_memory(pager.pages[i])
    
    num_additional_rows = table.num_rows % ROWS_PER_PAGE
    if num_additional_rows > 0:
        page_num = num_full_pages
        if pager.pages[page_num]:
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE)
            #free_memory(pager.pages[page_num])

    pager.file_descriptor.close()


def do_meta_command(input_buffer, table):
    if input_buffer.buffer == ".exit":
        db_close(table)
        exit(1)
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


def execute_insert(statement, table):
    if table.num_rows >= TABLE_MAX_PAGES:
        return EXECUTE_TABLE_FULL
    
    row_to_insert = statement.row_to_insert
    serialize_row(row_to_insert, row_slot(table, table.num_rows))
    table.num_rows += 1
    return EXECUTE_SUCCESS


def execute_select(statement, table):
    for i in range(table.num_rows):
        row = Row()
        deserialize_row(row_slot(table, i), row)
        print_row(row)
    return EXECUTE_SUCCESS


def execute_statement(statement, table):
    if statement.type == STATEMENT_INSERT:
        return execute_insert(statement, table)
    elif statement.type == STATEMENT_SELECT:
        return execute_select(statement, table)


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
    main(sys.argv)
