import pandas as pd

def rename_fields(df):
    """
    Rename each field name to a longer form version.
    """
    df_renamed = df.rename(columns={
        'pos': 'position',
        'team': 'team_name',
        'p': 'games_played',
        'w1': 'home_wins',
        'd1': 'home_draws',
        'l1': 'home_losses',
        'gf1': 'home_goals_for',
        'ga1': 'home_goals_against',
        'w2': 'away_wins',
        'd2': 'away_draws',
        'l2': 'away_losses',
        'gf2': 'away_goals_for',
        'ga2': 'away_goals_against',
        'gd': 'goal_difference',
        'pts': 'points',
        'date': 'match_date'
    })
    return df_renamed

def calculate_points(df):
    """
    Use the home and away columns to calculate the points, and
    deduct 10 points from Everton FC due to PSR violations starting 
    from November 2023.
    """
    # Calculate points normally for all rows
    df['points'] = (
        df['home_wins'] * 3 + df['away_wins'] * 3 +
        df['home_draws'] + df['away_draws']
    )

    # Calculate total wins, draws, and losses
    df['wins'] = df['home_wins'] + df['away_wins']
    df['draws'] = df['home_draws'] + df['away_draws']
    df['losses'] = df['home_losses'] + df['away_losses']
    
    df['goals_for'] = df['home_goals_for'] + df['away_goals_for']
    df['goals_against'] = df['home_goals_against'] + df['away_goals_against']

    # Convert the match_date from string to datetime for comparison
    df['match_date'] = pd.to_datetime(df['match_date'])
    
    
    return df 



def deduct_points_from_everton(df):
    """
    Deduct points for Everton FC if the match_date is in or after November 2023
    """
    psr_violation_start_date = pd.to_datetime('2023-11-01')
    everton_mask = (df['team_name'] == 'Everton') & (df['match_date'] >= psr_violation_start_date)
    df.loc[everton_mask, 'points'] -= 10
    
    return df

def drop_home_away_columns(df):
    """
    Drop/hide the home and away columns for user-friendliness 
    (after we've calculated the points from them).
    """
    columns_to_drop = [
        'home_wins', 'home_draws', 'home_losses',
        'home_goals_for', 'home_goals_against',
        'away_wins', 'away_draws', 'away_losses',
        'away_goals_for', 'away_goals_against'
    ]
    df_dropped = df.drop(columns=columns_to_drop)
    return df_dropped

def sort_and_reset_index(df):
    """
    Sort the dataframe based on the Premier League table standings rules
    and reset the 'position' column to reflect the new ranking.
    

    """
    # Sort by points, then goal difference, then goals for
    df_sorted = df.sort_values(by=['points', 'goal_difference', 'goals_for'], ascending=[False, False, False])

    # Reset the index to reflect the new ranking
    df_sorted = df_sorted.reset_index(drop=True)
    
    # Update the 'position' column to match the new index
    df_sorted['position'] = df_sorted.index + 1

    return df_sorted

def transform_data(df):
    """
    Apply all the transformation intents on the dataframe.
    """
    df_renamed = rename_fields(df)
    df_points_calculated = calculate_points(df_renamed)
    df_points_deducted = deduct_points_from_everton(df_points_calculated)
    
    # # Drop the columns related to home and away metrics to clean up the dataframe
    # df_cleaned = drop_home_away_columns(df_points_deducted)
    
    # Create the final dataframe with desired columns only
    df_cleaned = df_points_deducted[['position', 'team_name', 'games_played', 'wins', 'draws', 'losses', 'goals_for', 'goals_against', 'goal_difference', 'points', 'match_date']]
    
    # Sort the dataframe by points, goal_difference, and goals_for to apply the league standings rules
    df_final = df_cleaned.sort_values(by=['points', 'goal_difference', 'goals_for'], ascending=[False, False, False])

    # Reset the position column to reflect the new ranking after sorting
    df_final.reset_index(drop=True, inplace=True)
    df_final['position'] = df_final.index + 1

    return df_final
