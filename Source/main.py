
class InputBuffer:
    def __init__(self, buffer):
        self.buffer = buffer

enum = {}
enum["META_COMMAND_SUCCESS"]                = 0
enum["META_COMMAND_UNRECOGNIZED_COMMAND"]   = 1
enum["PREPARE_SUCCESS"]                 = 0
enum["PREPARE_UNRECOGNIZED_SUCCESS"]    = 1
enum["STATEMENT_INSERT"]    = 0
enum["STATEMENT_SELECT"]    = 1

class Statement:
    def __init__(self, type):
        self.type = type


def print_prompt():
    print "db >",

def read_input():
    buf = raw_input()
    return InputBuffer(buf)

def main():
    while True:
        print_prompt()
        input_buffer = read_input()

        if input_buffer.buffer[0] == ".":
            result = do_meta_command(input_buffer)
            if result == enum["META_COMMAND_SUCCESS"]:
                continue
            elif result == enum["META_COMMAND_UNRECOGNIZED_COMMAND"]:
                print "Unrecognized command '%s'." % input_buffer.buffer
                continue

        statement, result = prepare_statement(input_buffer)
        if result == enum["PREPARE_SUCCESS"]:
            pass
        elif result == enum["PREPARE_UNRECOGNIZED_SUCCESS"]:
            print "Unrecognized keyword at start of '%s'." % input_buffer.buffer
            continue

        execute_statement(statement)
        print "Executed."

def do_meta_command(input_buffer):
    if input_buffer.buffer == ".exit":
        exit(1)
    else:
        return enum["META_COMMAND_UNRECOGNIZED_COMMAND"]

def prepare_statement(input_buffer):
    if input_buffer.buffer[:6] == "insert":
        return Statement(enum["STATEMENT_INSERT"]), enum["PREPARE_SUCCESS"]
    if input_buffer.buffer == "select":
        return Statement(enum["STATEMENT_SELECT"]), enum["PREPARE_SUCCESS"]

    return None, enum["PREPARE_UNRECOGNIZED_SUCCESS"]

def execute_statement(statement):
    if statement.type == enum["STATEMENT_INSERT"]:
        print "This is where we should do a insert."
    elif statement.type == enum["STATEMENT_SELECT"]:
        print "This is where we should do a select."

main()
