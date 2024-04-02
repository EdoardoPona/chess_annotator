import os 
import pandas as pd 
from tqdm import tqdm
import argparse
import multiprocessing 
from concept_labelling.game_label import eval_game_moves 


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


if __name__=='__main__':
    args = parser.parse_args()
    df = pd.read_csv(args.dataset_path)
    pgn_column = args.pgn_column

    num_cores = multiprocessing.cpu_count()
    batch_size = 10000

    i = 0
    batch_df = df[i*batch_size:(i+1)*batch_size]
    for i in range(df.shape[0]//batch_size):
        print(f"Processing batch {i} of {df.shape[0]//batch_size}")
        batch_df = df[i*batch_size:(i+1)*batch_size]
        pool = multiprocessing.Pool(processes=num_cores)
        game_evals = pd.concat(
            tqdm(pool.imap(eval_game_moves, batch_df[pgn_column]), total=batch_df.shape[0]), 
            keys=batch_df.index, 
            names=['game', 'move']
        )
        game_evals.to_pickle(f"{os.environ['DATA_PATH']}/chess/lichess_100mb_annotated_{i}.pkl")
        # Close the multiprocessing pool
        pool.close()
        pool.join()

    # save the df to pickle, making sure the multi-index is preserved
    # make also sure the column multi-index is preserved

    game_evals.to_pickle(f"{os.environ['DATA_PATH']}/chess/lichess_100mb_annotated.pkl")


