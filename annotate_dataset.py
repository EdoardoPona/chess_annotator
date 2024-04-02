import os 
import pandas as pd 
from tqdm import tqdm
import argparse
import multiprocessing 
from stockfish_annotator.game_label import eval_game_moves 


parser = argparse.ArgumentParser(description='Annotate dataset with Stockfish')
parser.add_argument(
    '--dataset_path', 
    type=str, 
    default=f"{os.environ['DATA_PATH']}/chess/lichess_100mb.csv",
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


if __name__=='__main__':
    args = parser.parse_args()
    df = pd.read_csv(args.dataset_path)
    pgn_column = args.pgn_column
    num_cores = args.num_cores 
    batch_size = args.batch_size 

    i = 0
    batch_df = df[i*batch_size:(i+1)*batch_size]

    save_path = args.output_path if args.output_path is not None else args.dataset_path.replace('.csv', '_annotated_{batch}.pkl')
    for i in range(df.shape[0]//batch_size):
        print(f"Processing batch {i} of {df.shape[0]//batch_size}")
        batch_df = df[i*batch_size:(i+1)*batch_size]
        pool = multiprocessing.Pool(processes=num_cores)
        game_evals = pd.concat(
            tqdm(pool.imap(eval_game_moves, batch_df[pgn_column]), total=batch_df.shape[0]), 
            keys=batch_df.index, 
            names=['game', 'move']
        )
        game_evals.to_pickle(save_path.format(batch=i))
        # Close the multiprocessing pool
        pool.close()
        pool.join()

    # save the df to pickle, making sure the multi-index is preserved
    # make also sure the column multi-index is preserved

