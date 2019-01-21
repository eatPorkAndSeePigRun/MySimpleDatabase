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


# TODO size_of_attribute = lambda struct, attribute : len()
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


class Table:
    def __init__(self, pages=["" for i in range(TABLE_MAX_PAGES)], num_rows=0):
        self.pages = pages
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

    table, page_num, byte_offset = dest
    old_page = table.pages[page_num]
    new_page = old_page[:byte_offset] + temp + old_page[byte_offset+len(temp):]
    table.pages[page_num] = new_page    


def deserialize_row(src, dest):
    table, page_num, byte_offset = src
    page = table.pages[page_num]

    temp = page[byte_offset:]
    dest.id = struct.unpack("i", temp[ID_OFFSET : ID_OFFSET+ID_SIZE])[0]
    dest.username = struct.unpack("%ds" % USERNAME_SIZE, temp[USERNAME_OFFSET : USERNAME_OFFSET+USERNAME_SIZE])[0]
    dest.email = struct.unpack("%ds" % EMAIL_SIZE, temp[EMAIL_OFFSET : EMAIL_OFFSET+EMAIL_SIZE])[0]


def row_slot(table, row_num):
    page_num = row_num / ROWS_PER_PAGE
    if not table.pages[page_num]:
        table.pages[page_num] = "\x00" * PAGE_SIZE
    row_offset = row_num % ROWS_PER_PAGE
    byte_offset = row_offset * ROW_SIZE
    return table, page_num, byte_offset


def print_prompt():
    print "db >",


def read_input():
    buf = raw_input()
    return InputBuffer(buf)


def do_meta_command(input_buffer):
    if input_buffer.buffer == ".exit":
        exit(0)
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
#    statement = Statement()
#    if input_buffer.buffer[:6] == "insert":
#        statement.type = STATEMENT_INSERT
#        args = input_buffer.buffer.split(" ", 3)
#        if len(args) < 4:
#            return statement, PREPARE_SYNTAX_ERROR
#        statement.row_to_insert.id = int(args[1])
#        statement.row_to_insert.username = args[2]
#        statement.row_to_insert.email = args[3]
#        return statement, PREPARE_SUCCESS
#    if input_buffer.buffer == "select":
#        statement.type = STATEMENT_SELECT
#        return statement, PREPARE_SUCCESS

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


def main():
    table = Table()
    while True:
        print_prompt()
        input_buffer = read_input()

        if input_buffer.buffer[0] == ".":
            result = do_meta_command(input_buffer)
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


main()
