import chess
import chess.pgn
import io
import pandas as pd 
import numpy as np
from stockfish_annotator.stockfish_utils import get_stockfish_eval, new_game_context


def get_game_states(pgn_string: str, return_fen=False) -> list[chess.Board | str]:
    ''' given a pgn string, return a list of game states (possibly in fen notation)'''
    game = chess.pgn.read_game(io.StringIO(pgn_string))
    node = game.end()
    board_states = []
    while node.parent is not None:
        board_states.append(node.board())
        node = node.parent
    board_states.reverse()
    if return_fen:
        board_states = [board.fen() for board in board_states]
    return board_states


def eval_game_moves(pgn: str):
    ''' given the pgn string of a game, return the state evaluation for each move '''
    states = get_game_states(pgn, return_fen=True)
    with new_game_context() as game_process:
        game_evals = pd.concat([
            parse_eval(
                get_stockfish_eval(s, game_process),
                return_dataframe=True
            )
            for s in states
        ], ignore_index=True)
    return game_evals


def parse_eval(eval: list[str], return_dataframe: bool=True) -> pd.DataFrame | dict:
    ''' 
    given a list of strings representing stdout from stockfish eval on a single position, 
    returns a dictionary, or a dataframe the dataframe is indexed by a multi-index, 
    with concept_name, player, game_phase  
    '''
    parsed_out = {}
    for line in eval[3:]:  # the first 3 are headers 
        try:
            concept, white, black, total = line.split('|')
            concept_data = {}
            players = ['white', 'black', 'total']
            scores = [white, black, total]
            for player, scores in zip(players, scores):
                # some are split with a single space, some with multiple spaces
                scores = [s for s in scores.strip().split(' ') if s != '']
                mg, eg = scores
                try:
                    mg, eg = float(mg), float(eg)
                except:
                    mg, eg = np.nan, np.nan
                concept_data[player] = {
                    'mg': mg,
                    'eg': eg
                }
            parsed_out[concept] = concept_data
        except Exception as e:
            pass

    if return_dataframe:
        parsed_out = pd.concat(
            {
                (i,j,k): pd.Series(parsed_out[i][j][k]) for i in parsed_out.keys() 
                for j in parsed_out[i].keys() for k in parsed_out[i][j].keys()
            }, 
            axis=1
        )
        parsed_out.columns.names = ['concept_name', 'player', 'game_phase']

    return parsed_out


        
