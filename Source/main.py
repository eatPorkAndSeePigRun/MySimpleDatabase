
class InputBuffer:
    def __init__(self, buffer):
        self.buffer = buffer

def print_prompt():
    print "db >",

def read_input():
    buf = raw_input()
    return InputBuffer(buf)

def main():
    while True:
        print_prompt()
        input_buffer = read_input()

        if input_buffer.buffer == ".exit":
            exit(1)
        else:
            print "Unrecognized command '%s'." % input_buffer.buffer


main()
