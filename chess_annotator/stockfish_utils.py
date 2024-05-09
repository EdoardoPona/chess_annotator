import subprocess
import os 
from contextlib import contextmanager


STOCKFISH_PATH = os.environ.get('STOCKFISH_PATH')


def get(process) -> str:
    ''' Function to read lines from Stockfish's output '''
    return process.stdout.readline().strip()


def get_multiline(
        process, 
        n_lines: int = None,
    ) -> list[str]:
    ''' gets print from a multiline output '''
    out = []
    done = False
    i = 0
    while not done:
        line = get(process)
        if line == "" and n_lines is None:
            break
        out.append(line)
        done = False if n_lines is None else i >= n_lines
        i += 1
    return out 


def display_board(process) -> list[str]:
    put(process, "d")
    display_out = get_multiline(process, n_lines=19)
    return display_out


def put(process, command) -> None:
    ''' Function to send commands to Stockfish '''
    process.stdin.write(command + '\n')
    process.stdin.flush()


def new_game(fen: str = None) -> subprocess.Popen:
    assert STOCKFISH_PATH is not None, "Stockfish path not set"
    process = subprocess.Popen(
        STOCKFISH_PATH, 
        universal_newlines=True, 
        stdin=subprocess.PIPE, 
        stdout=subprocess.PIPE, 
        bufsize=-1
    )
    put(process, "uci")
    while get(process) != "uciok":
        continue
    put(process, "ucinewgame")
    if fen is not None:
        put(process, f"position fen {fen}")
    return process


@contextmanager
def new_game_context(fen: str = None):
    process = new_game(fen)
    try:
        yield process
    finally:
        put(process, "quit")
        process.communicate()


def close_game_process(process: subprocess.Popen) -> None:
    put(process, "quit")
    process.communicate()


def get_stockfish_eval(fen: str | None = None, process: subprocess.Popen | None = None) -> list[str]:
    if fen is None and process is None:
        raise ValueError("Must provide either a FEN string or a process object")

    started_process = False 
    if process is None:
        process = new_game(fen)
        started_process = True
    else:
        # if we received a process and a fen, we assume there might be a game already running
        # in which case we need tell stockfish to reset it before we put the position
        put(process, "ucinewgame")
        put(process, f"position fen {fen}")
        # TODO how much speedup do we get if we simply make a move instead? 


    # Send the eval command and print output
    put(process, "eval")

    # we must give the explicit number of output lines
    # otherwise we will read pieces of the previous outpout if using the same process
    eval_output = get_multiline(process, n_lines=20)
    # print('#'*100)
    # for line in eval_output:
    #     print(line)
    
    if started_process:
        close_game_process(process)

    return eval_output


# TODO store constants for the number of output lines for each relevant command
