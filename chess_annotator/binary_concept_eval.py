import chess
import re
import itertools
import collections
import pandas as pd 


class CallOnDomain:
    ''' 
    Decorator that allows to specify the domain of a function, 
    and evaluates the function on all the elements of the domain that are not pinned. 
    '''
    # NOTE: for this to work we need the function to be called with kwargs only 

    def __init__(self, **kwargs):
        ''' 
        args is a list of collections (lists) of possible inputs to a function 
        '''
        # checking that each argument is an iterable
        assert all(isinstance(arg, collections.abc.Iterable) for arg in kwargs.values())

        # this is the full domain of the functoin (as far as we know)
        # the user can possibly pin some values and reduce the wrapped function's domain
        self.domain_kwarg_dict = kwargs
        self.domain = self._get_domain(self.domain_kwarg_dict)

    def _get_domain(self, free_kwarg_dict: dict[list]):
        keys, values = zip(*free_kwarg_dict.items())
        domain = itertools.product(*values)   
        # add the keys back in 
        return map(lambda x: dict(zip(keys, x)), domain)

    def __call__(self, f):

        def wrapper(*args, **pinned_kwargs):
            assert len(args) == 0, "This decorator only works with keyword arguments"

            # for the keys which are present in both, we pin them to the values in kwargs
            # restrict the free keys to the ones that are not pinned
            free_kwarg_dict = self.domain_kwarg_dict.copy()
            free_keys = set(free_kwarg_dict.keys())
            pinned_keys = set(pinned_kwargs.keys())
            # if pinned keys contains all of free keys
            if pinned_keys.issuperset(free_keys):
                out = f(**pinned_kwargs) 
                return out 
            else:
                for pinned_key, pinned_value in pinned_kwargs.items():
                    free_kwarg_dict[pinned_key] = [pinned_value]

                f.domain = self._get_domain(free_kwarg_dict)
                # yielding inside this new generator function is a hack so that the wrapper functoin does not 
                # exclusively return generators 
                def generator_func():
                    for i in f.domain:
                        # yield the free arguments and the evaluated function 
                        yield {
                            'free_inputs': {k:i[k] for k in free_keys-pinned_keys}, 
                            'output': f(**i)
                        }
                return generator_func()

        # this is a flag that tells us that the function is a CallOnDomain function
        wrapper.is_call_on_domain = True        
        return wrapper


@CallOnDomain(color=chess.COLORS, square=chess.SQUARES)
def is_attacked(board: chess.Board, color: bool, square: int) -> bool:
    ''' Returns True if the square is attacked by the color. '''
    return len(board.attackers(color, square)) > 0


@CallOnDomain(color=chess.COLORS, square=chess.SQUARES)
def is_pinned(board: chess.Board, color: bool, square: int) -> bool:
    return board.is_pinned(color, square)


# TODO this might return None - do we treat it as a multiclass? 
@CallOnDomain(square=chess.SQUARES)
def color_at(board: chess.Board, square: int) -> int:
    return board.color_at(square)


@CallOnDomain(color=chess.COLORS)
def has_castling_rights(board: chess.Board, color: bool) -> bool:
    return board.has_castling_rights(color)


@CallOnDomain(color=chess.COLORS)
def has_insufficient_material(board: chess.Board, color: bool) -> bool:
    return board.has_insufficient_material(color)


@CallOnDomain(color=chess.COLORS)
def has_kingside_castling_rights(board: chess.Board, color: bool) -> bool:
    return board.has_kingside_castling_rights(color)


@CallOnDomain(color=chess.COLORS)
def has_queenside_castling_rights(board: chess.Board, color: bool) -> bool:
    return board.has_queenside_castling_rights(color)


def has_legal_en_passant(board: chess.Board) -> bool:
    return board.has_legal_en_passant()


def can_claim_draw(board: chess.Board) -> bool:
    return board.can_claim_draw()



def evaluate_binary_concepts(board: chess.Board, binary_concepts: dict = None):
    if binary_concepts is None:
        binary_concepts = BINARY_CONCEPT_REGISTRY

    concept_values = {}
    for name, fn in binary_concepts.items():
        # this is a wrapped function being evaluated on the whole (unpinned) domain 
        if hasattr(fn, 'is_call_on_domain'): 
            values = list(fn(board=board))
            for i in range(len(values)):
                free_inputs = values[i]['free_inputs']
                suffix = '.'.join(f"{key}_{value}" for key, value in free_inputs.items())
                concept_values[f"{name}.{suffix}"] = values[i]['output']
        else:
            output = fn(board)
            concept_values[name] = output
    return concept_values


def eval_game_moves_binary(pgn_string: str, return_dataframe=True):
    # remove whenever there is a number followed by a dot
    pgn_string = re.sub(r'[0-9]+\.', '', pgn_string)

    board = chess.Board()
    moves = pgn_string.split(' ')

    board_state_concept_values = {}
    # 0 is the first move, not the starting position 
    for i, move in enumerate(moves):
        board.push_san(move)
        board_state_concept_values[i] = evaluate_binary_concepts(board)
    
    if return_dataframe:
        return pd.DataFrame(
            data=board_state_concept_values.values(), 
            index=board_state_concept_values.keys()
        )
    return board_state_concept_values



BINARY_CONCEPT_REGISTRY = {
    'is_attacked': is_attacked,
    'is_pinned': is_pinned,
    'color_at': color_at,
    'has_castling_rights': has_castling_rights,
    'has_insufficient_material': has_insufficient_material,
    'has_kingside_castling_rights': has_kingside_castling_rights,
    'has_queenside_castling_rights': has_queenside_castling_rights,
    'has_legal_en_passant': has_legal_en_passant,
    'can_claim_draw': can_claim_draw
}


