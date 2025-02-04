#!/usr/bin/env python3

import subprocess
import sys
import argparse
import logging
import os

def setup_logging():
    """
    Sets up the logging configuration for the wrapper script.
    """
    logging.basicConfig(
        filename='run_all_leagues.log',
        level=logging.INFO,  # Change to DEBUG for more detailed logs
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def parse_arguments():
    """
    Parses command-line arguments for the wrapper script.
    """
    parser = argparse.ArgumentParser(description="Run insert_players.py for multiple leagues.")
    parser.add_argument(
        '--season',
        type=str,
        default=None,
        help='Season (e.g., 2024/2025). If specified, all seasons up to and including this season will be processed for all leagues. If omitted, all seasons will be processed.'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Run insert_players.py for all leagues in parallel. Default is sequential execution.'
    )
    return parser.parse_args()

def main():
    setup_logging()
    args = parse_arguments()
    
    league_names = [
        'albania_superliga',
        'czech_republic_first_league',
        'france_ligue_2',
        'latvia_virsliga',
        'romania_liga_i',
        'turkey_super_lig',
        'armenia_armenian_premier_league',
        'denmark_superliga',
        'england_premier_league',
        'germany_2_bundesliga',
        'moldova_moldovan_national_division',
        'scotland_premiership',
        'ukraine_ukrainian_premier_league',
        'austria_bundesliga',
        'estonia_meistriliiga',
        'germany_bundesliga',
        'netherlands_eerste_divisie',
        'serbia_superliga',
        'azerbaijan_premyer_liqasi',
        'greece_super_league',
        'netherlands_eredivisie',
        'slovakia_super_lig',
        'belgium_pro_league',
        'england_championship',
        'hungary_nb_i',
        'norway_eliteserien',
        'spain_la_liga',
        'bulgaria_first_league',
        'italy_serie_a',
        'poland_ekstraklasa',
        'spain_segunda_division',
        'england_efl_league_one',
        'finland_veikkausliiga',
        'italy_serie_b',
        'portugal_liga_nos',
        'sweden_allsvenskan',
        'croatia_prva_hnl',
        'england_efl_league_two',
        'france_ligue_1',
        'kazakhstan_kazakhstan_premier_league',
        'portugal_liga_pro',
        'switzerland_super_league',
        'england_championship',
        'england_fa_cup',
        'england_league_cup',
        'england_community_shield',
        'europe_uefa_europa_league',
        'europe_uefa_champions_league',
        'europe_uefa_europa_conference_league'
    ]
    
    # Remove duplicate entries if any
    league_names = list(dict.fromkeys(league_names))
    
    # Path to the main script
    script_path = '/root/barnard/scripts/daily-automatic/update-players.py'
    
    if not os.path.exists(script_path):
        logging.error(f"Main script not found at path: {script_path}")
        sys.exit(1)
    
    for league in league_names:
        logging.info(f"Starting processing for league: {league}")
        
        # Build the command
        command = ['python', script_path, league]
        
        if args.season:
            command.extend(['--season', args.season])
        
        # Log the command being executed
        logging.info(f"Executing command: {' '.join(command)}")
        
        try:
            if args.parallel:
                # Run the process in the background
                subprocess.Popen(command)
                logging.info(f"Started parallel process for league: {league}")
            else:
                # Run the process and wait for it to complete
                result = subprocess.run(command, capture_output=True, text=True)
                
                # Log stdout and stderr
                if result.stdout:
                    logging.info(f"Output for league {league}:\n{result.stdout}")
                if result.stderr:
                    logging.error(f"Errors for league {league}:\n{result.stderr}")
                
                if result.returncode != 0:
                    logging.error(f"insert_players.py exited with return code {result.returncode} for league {league}")
                else:
                    logging.info(f"Successfully processed league: {league}")
        except Exception as e:
            logging.error(f"Failed to execute insert_players.py for league {league}: {e}")
    
    logging.info("All leagues have been processed.")

if __name__ == "__main__":
    main()
