import sys


def addr_from_pid(pid) -> tuple[str, int]:
    ip, port = pid.split(":")
    return (ip, int(port))

def check_input(to_close):
    """checks input for program commands continuously"""

    print("[CONFIG] Help: \n\t'q' to [q]uit\n\t'c' to [c]rash\n\t'v' to toggle [v]erbosity\n\n")
    while True:
        text = input()
        check_single_input(text, to_close)


def check_single_input(text, to_close):
    """checks a single input for program commands"""

    if text == "q":
        print("[CONFIG] [q]uitting")
        if to_close and callable(getattr(to_close,'close')):
            to_close.close()
        else:
            print("[CONFIG] no close method found")

        sys.exit(0)

    elif text == "c":
        print("[CONFIG] [c]rashing")
        sys.exit(1)

    elif text.startswith('v'):
        if len(text) == 2 and text[1].isdigit() and 0 <= int(text[1]) <= 3:
            level = int(text[1])
            global LOGGING_LEVEL
            LOGGING_LEVEL = level
            print("[CONFIG] set verbosity to:", level)
        else:
            print("[CONFIG] invalid verbosity level")
