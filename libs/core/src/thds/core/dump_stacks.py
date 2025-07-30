import signal
import sys
import threading
import traceback


def dump_all_stacks(signum, frame):
    print(f"\n=== Stack dump triggered by signal {signum} ===", flush=True)
    for thread in threading.enumerate():
        print(f"\nThread: {thread.name} (ID: {thread.ident})", flush=True)
        print("-" * 50, flush=True)
        if thread.ident in sys._current_frames():
            traceback.print_stack(sys._current_frames()[thread.ident], file=sys.stdout)
        else:
            print("No frame available for this thread", flush=True)
    print("=== End stack dump ===\n", flush=True)


def setup_signal_handler(signal_num: int = signal.SIGUSR1):
    """
    Set up a signal handler to dump all thread stacks when the specified signal is received.
    Default is SIGUSR1, but can be changed to any other signal as needed.
    """
    signal.signal(signal_num, dump_all_stacks)

    print(
        f"Signal handler set up for signal {signal_num}."
        " Send this signal to the process to dump all thread stacks."
    )
