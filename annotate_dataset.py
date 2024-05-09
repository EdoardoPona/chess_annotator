import os 
import pandas as pd 
from tqdm import tqdm
import argparse
import multiprocessing 
from chess_annotator.continuous_concept_eval import eval_game_moves_continuous
from chess_annotator.binary_concept_eval import eval_game_moves_binary


CONCEPT_TYPES = {'binary', 'continuous'}


parser = argparse.ArgumentParser(description='Annotate a chess game dataset')
parser.add_argument(
    '--dataset_path', 
    type=str, 
    default=f"{os.environ['DATA_PATH']}datasets/chess/datasets/lichess_100mb.csv",
    help='Path to the dataset'
)
parser.add_argument(
    '--pgn_column',
    type=str,
    default='transcript',
    help='Name of the column in the dataframe containing the PGN strings'
)
parser.add_argument(
    '--num_cores',
    type=int,
    default=multiprocessing.cpu_count(),
    help='Number of cores to use for multiprocessing'
)
parser.add_argument(
    '--batch_size',
    type=int,
    default=10000,
    help='Number of games to process in a single batch'

)
parser.add_argument(
    '--output_path',
    type=str,
    default=None,
    help='Path to save the annotated dataset'
)
parser.add_argument(
    '--concept_type',
    type=str,
    default='binary',
    help=f'one of {CONCEPT_TYPES}'
)


if __name__=='__main__':
    args = parser.parse_args()
    df = pd.read_csv(args.dataset_path)
    pgn_column = args.pgn_column
    num_cores = args.num_cores 
    batch_size = args.batch_size 
    concept_type = args.concept_type
    assert concept_type in CONCEPT_TYPES
    concept_eval_function = eval_game_moves_continuous if concept_type == 'continuous' else eval_game_moves_binary 

    i = 0
    batch_df = df[i*batch_size:(i+1)*batch_size]

    default_save_path = args.dataset_path.replace('.csv', '_annotated/{concept_type}/batch_{batch}.pkl')
    save_path = args.output_path if args.output_path is not None else default_save_path
    for i in range(df.shape[0]//batch_size):
        print(f"Processing batch {i} of {df.shape[0]//batch_size}")
        batch_df = df[i*batch_size:(i+1)*batch_size]
        pool = multiprocessing.Pool(processes=num_cores)
        game_evals = pd.concat(
            tqdm(pool.imap(concept_eval_function, batch_df[pgn_column]), total=batch_df.shape[0]), 
            keys=batch_df.index, 
            names=['game', 'move']
        )

        # TODO add the state pgn to the dataframes 
        # make the save directory if it doesn't already exist 
        batch_save_path = save_path.format(concept_type=concept_type, batch=i)
        os.makedirs(os.path.dirname(batch_save_path), exist_ok=True)
        game_evals.to_pickle(batch_save_path)
        # Close the multiprocessing pool
        pool.close()
        pool.join()

    # save the df to pickle, making sure the multi-index is preserved
    # make also sure the column multi-index is preserved

