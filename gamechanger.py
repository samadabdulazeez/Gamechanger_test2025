# 📌 Full Script: GameChanger ETL & EDA Process

# This script combines all cells from the GameChanger project into a single executable file.
# It performs data extraction, cleaning, transformation, calculation of advanced statistics,
# and generates various visualizations for basketball player and team performance.

# --- Required Libraries ---
import pandas as pd  # Data manipulation & Excel reading
import numpy as np  # Numeric processing
import matplotlib
matplotlib.use('Agg') # Set the Matplotlib backend to 'Agg' for non-interactive plotting
import matplotlib.pyplot as plt  # Visualization
import seaborn as sns  # Advanced visualization
import os  # File path management
import re # Regular expressions for robust string matching
import requests # For downloading files from URLs
import sys  # For command-line arguments

# --- Global Variables & Configuration ---
# ✅ League configuration: Map league names to Google Sheet IDs and output file names
LEAGUE_CONFIG = {
    "Rising Stars S1": {
        "google_sheet_id": "1ou7oCpd6QNe4STDPpzMpuv08V15eZJy4ERSVItf_T64",
        "cleaned_data_file": "Final_Cleaned_Data.csv",
        "advanced_stats_file": "Final_Player_Advanced_Stats.csv",
        "local_file_name": "Box Scores Rising Star.xlsx"
    },
    "Rising Stars S2": {
        "google_sheet_id": "1UCAvB_iS0Mi5j5CNErAsS2-aKjGJTBTOFpEVQpQ8ZzY",
        "cleaned_data_file": "Final_Cleaned_Data_Unknown_League.csv",
        "advanced_stats_file": "Final_Player_Advanced_Stats_Unknown_League.csv",
        "local_file_name": "Box Scores Rising Star S2.xlsx"
    }
}

# ✅ Get league from command-line argument or default to "Rising Stars S1"
selected_league = sys.argv[1] if len(sys.argv) > 1 else "Rising Stars S1"

# Validate league name
if selected_league not in LEAGUE_CONFIG:
    print(f"⚠️ Warning: Unknown league '{selected_league}'. Defaulting to 'Rising Stars S1'.")
    selected_league = "Rising Stars S1"

# ✅ Set global variables based on selected league
league_config = LEAGUE_CONFIG[selected_league]
google_sheet_id = league_config["google_sheet_id"]
google_sheet_download_url = f"https://docs.google.com/spreadsheets/d/{google_sheet_id}/export?format=xlsx"
local_file_name = league_config["local_file_name"]
cleaned_data_file = league_config["cleaned_data_file"]
advanced_stats_file = league_config["advanced_stats_file"]

print(f"📊 Processing league: {selected_league}")
print(f"📁 Output files: {cleaned_data_file}, {advanced_stats_file}")

# Define the standard stats columns expected in the box score data *after* cleaning and parsing.
# These are the target column names for the combined DataFrame.
stats_cols_excel_headers = [
    'MIN', 'PTS', 'FGM', 'FGA', 'FG_PCT',
    '3PTM', '3PA', '3P%', 'FTM', 'FTA', 'FT%',
    'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF'
]

# Define known base team labels (these are keywords to look for if direct extraction fails)
# Note: Teams are also automatically extracted from sheet names (e.g., "Purple vs U11" -> Purple, U11)
# Also include variations like "14U" which appears in data but sheet name has "U14"
team_labels = ["Blue", "White", "Beige", "Black", "Grey", "Purple", "U11", "U14", "14U"]

# Regex pattern for box score sheet names (e.g., "Team1 vs Team2", "Team1 vs Team2 2")
# This pattern will match any two words separated by " vs " or " Vs ", followed by an optional suffix.
BOX_SCORE_SHEET_PATTERN = r'^(\w+)\s+vs\s+(\w+)(.*)?$'


# Initialize global DataFrames (these will be populated by the functions)
df_combined = pd.DataFrame()
df_visual = pd.DataFrame()
player_advanced_stats_avg = pd.DataFrame()
xls = None # Initialize xls globally as it's set in load_and_explore_data
df_preview = None # Initialize df_preview globally
sheet_team_name_lookup = {} # Initialize sheet_team_name_lookup globally
df_team_rows = None # Initialize df_team_rows globally




# --- Function Definitions (All moved to top for proper scoping) ---

# 📌 Cell 1: Load & Explore Game Box Scores from Excel
def load_and_explore_data():
    global xls, df_preview # Declare global to modify the xls and df_preview objects
    print("--- Starting Data Loading from Google Sheets ---")

    # ✅ Attempt to download the Excel file from Google Sheets
    try:
        print(f"Attempting to download '{local_file_name}' from Google Sheets...") 
        response = requests.get(google_sheet_download_url)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        # Overwrite the file if it exists, otherwise create it
        with open(local_file_name, 'wb') as f:
            f.write(response.content)
        print(f"✅ Successfully downloaded '{local_file_name}'.")

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error downloading the file from Google Sheets: {e}")
        raise # Re-raise the exception to stop execution if download fails
    except Exception as e:
        print(f"An unexpected error occurred during file download: {e}")
        raise # Re-raise any other unexpected exceptions

    # ✅ Ensure the Excel file exists before proceeding (it should now, if download succeeded)
    if not os.path.exists(local_file_name):
        raise FileNotFoundError(f"⚠️ Local Excel file not found: {local_file_name}. Download failed or file path is incorrect.")

    # ✅ Load the Excel workbook as a pd.ExcelFile object for dynamic sheet access
    xls = pd.ExcelFile(local_file_name)

    # ✅ Display all available sheet names from the Excel file
    print("\n📜 Available Sheets in Excel File:", xls.sheet_names)

    # ✅ Load the first *non-acronyms* and *non-empty* sheet to inspect initial structure for preview
    df_preview_local = None
    preview_sheet_name = None
    for sheet_name in xls.sheet_names:
        if sheet_name == "Acronyms": # Skip the acronyms sheet for preview purposes
            continue

        try:
            df_temp = pd.read_excel(xls, sheet_name=sheet_name)
            if not df_temp.empty:
                df_preview_local = df_temp
                preview_sheet_name = sheet_name
                break # Found a suitable sheet for preview
        except Exception as e:
            print(f"⚠️ Could not read sheet '{sheet_name}' for preview: {e}")
            continue

    if df_preview_local is None:
        print("No suitable (non-Acronyms, non-empty) sheets found for initial preview.")
    else:
        df_preview = df_preview_local
        print(f"\n🔍 First Few Rows from Sheet (preview): {preview_sheet_name}")
        print(df_preview.head())

        # ✅ Check for potential team assignments in raw data
        print("\n🚀 Unique Values in First Column (Possible Team Names, from preview sheet):")
        if not df_preview.empty and df_preview.shape[1] > 0:
            print(df_preview.iloc[:, 0].dropna().unique())
        else:
            print("Preview DataFrame is empty or has no columns to check for team names.")

    print("--- Data Loading Setup Complete ---")


# 📌 Cell 2: Enhanced Team Name Extraction for General Sheets
def extract_team_names():
    global sheet_team_name_lookup, df_team_rows # Declare global
    print("--- Starting Team Name Extraction for Lookup ---\n")

    # ✅ Prepare a dictionary to store detected team names per sheet
    sheet_team_name_lookup = {}

    # Iterate through all sheets in the Excel file to pre-identify team names
    for sheet_name in xls.sheet_names:
        if sheet_name == "Acronyms" or sheet_name == "Sheet3": # Skip metadata sheets and potentially empty 'Sheet3'
            continue

        # Try to parse team names directly from the sheet name (e.g., "Blue vs White")
        # Using re.IGNORECASE for 'vs' and 'Vs'
        match_vs = re.match(r'(\w+)\s+vs\s+(\w+)', sheet_name, re.IGNORECASE)
        if match_vs:
            team1 = match_vs.group(1).title() # Convert to title case for consistency
            team2 = match_vs.group(2).title()
            sheet_team_name_lookup[sheet_name] = f"{team1}, {team2}"
            print(f"  Detected multi-team from sheet name: '{sheet_name}' -> '{team1}, {team2}'")
            continue

        # Otherwise, try dynamic detection from sheet content for single teams or less structured names
        try:
            df_raw = pd.read_excel(xls, sheet_name=sheet_name)
            if not df_raw.empty and df_raw.shape[1] > 0:
                detected_teams_in_sheet = []
                # Look for team labels in the first column (assuming metadata might be there)
                # Check first 5 rows to be safe
                for i in range(min(5, len(df_raw))):
                    first_col_val = str(df_raw.iloc[i, 0]).lower() if not pd.isna(df_raw.iloc[i, 0]) else ''
                    for team in team_labels:
                        if team.lower() in first_col_val and team not in detected_teams_in_sheet:
                            detected_teams_in_sheet.append(team)

                if detected_teams_in_sheet:
                    sheet_team_name_lookup[sheet_name] = ", ".join(sorted(detected_teams_in_sheet))
                    print(f"  Detected single/implied team from content: '{sheet_name}' -> '{sheet_team_name_lookup[sheet_name]}'")
            else:
                print(f"  No explicit team detected for sheet '{sheet_name}' from name or content header. Will rely on Player-Team combinations later.")

        except Exception as e:
            print(f"⚠️ Error reading sheet '{sheet_name}' for team extraction: {e}")
            continue

    # Display the identified team names per sheet
    print("\n🚀 Identified Team Names per Sheet (for lookup during cleaning):")
    if sheet_team_name_lookup:
        for sheet, teams in sheet_team_name_lookup.items():
            print(f"  {sheet}: {teams}")
    else:
        print("No specific team names identified from sheets (e.g., 'Team1 vs Team2' pattern or direct team label in first column).")

    # Added from gamechanger_refined_etl_eda Cell 2 for `df_team_rows` logic
    all_team_rows_local = []
    for sheet_name in xls.sheet_names:
        df_raw = pd.read_excel(xls, sheet_name=sheet_name)
        team_rows = df_raw.copy()
        team_rows = team_rows[team_rows.apply(lambda row: any(team in str(row.values) for team in team_labels), axis=1)].copy()

        if not team_rows.empty:
            team_rows.insert(0, "Sheet Name", sheet_name)
            all_team_rows_local.append(team_rows)

    df_team_rows = pd.concat(all_team_rows_local, ignore_index=True) if all_team_rows_local else None

    if df_team_rows is not None:
        # Attempt to find the column containing the team names, often the first non-Sheet Name column
        team_col_detected_name = None
        for col in df_team_rows.columns:
            if col != 'Sheet Name' and df_team_rows[col].astype(str).str.contains('|'.join(team_labels), case=False).any():
                team_col_detected_name = col
                break

        if team_col_detected_name:
            # Standardize team names in the detected column
            df_team_rows[team_col_detected_name] = df_team_rows[team_col_detected_name].replace({
                "Key Players To Watch Out For (Team White)": "White",
                "Defensive Plan": "White"
            }).str.strip().str.title()

            # Specific override for "Blue vs White" sheet if needed, to ensure correct team names appear for subsequent processing
            # This logic should be handled downstream in clean_single_box_score_sheet for individual player assignments
            # For now, just ensure the raw detection is reasonable.

    print("--- Team Name Extraction Complete ---")

# 📌 Cell 3: Core ETL Process (Dynamic Sheet Processing)
def process_box_scores():
    global df_combined # Declare global to modify df_combined
    print("\n--- Starting ETL: Dynamic Sheet Processing ---")

    def clean_single_box_score_sheet(df_sheet_raw, sheet_name, team_labels):
        """
        Cleans a single box score DataFrame, identifies headers, assigns teams,
        and converts columns. This version robustly handles team assignment by
        scanning the entire sheet for team headers before extracting player data.
        It also correctly parses FG, 3PT, FTA from 'X-Y' format into FGM/FGA, 3PTM/3PA, FTM/FTA.
        """
        df = df_sheet_raw.copy()

        if df.empty:
            print(f"⚠️ Sheet '{sheet_name}' is empty, skipping.")
            return None

        # Headers to look for to identify the start of player data
        # These are the raw column names expected in the Excel sheet
        expected_player_data_headers_raw = ["Player No.", "PTS", "FG", "FG_PCT", "3PT", "3P%", "FTA", "FT%", "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "PF"]

        header_row_idx = None
        team_header_locations = []
        base_team_names_lower = {t.lower(): t.title() for t in team_labels}
        # Normalize team name variations: "14U" -> "U14" for consistency
        base_team_names_lower["14u"] = "U14"

        for i in range(len(df)):
            row_values = df.iloc[i].astype(str).str.lower().values
            first_col_value_raw = df.iloc[i, 0]
            normalized_first_col_val = str(first_col_value_raw).strip().lower()

            # Check for player data header row by looking for common raw headers
            if header_row_idx is None and any(col.lower() in val for val in row_values for col in expected_player_data_headers_raw):
                header_row_idx = i
            
            if normalized_first_col_val in base_team_names_lower:
                team_header_locations.append((i, base_team_names_lower[normalized_first_col_val]))

        if header_row_idx is None:
            print(f"⚠️ No valid data header found in '{sheet_name}'. Skipping as a non-box score sheet.")
            return None

        processed_player_records = []
        current_team_for_players = None

        # Get the actual headers from the detected header row in the raw data
        original_raw_headers = [str(col).strip() if pd.notna(col) else f"Unnamed: {i}" for i, col in enumerate(df_sheet_raw.iloc[header_row_idx].values)]
        raw_col_name_to_idx = {col: i for i, col in enumerate(original_raw_headers)}

        for i in range(header_row_idx + 1, len(df_sheet_raw)):
            row_data = df_sheet_raw.iloc[i].copy()

            first_col_value_raw = row_data.iloc[0]
            normalized_first_col_val = str(first_col_value_raw).strip().lower()
            if normalized_first_col_val in base_team_names_lower:
                current_team_for_players = base_team_names_lower[normalized_first_col_val]
                continue

            relevant_team_headers = [name for idx, name in team_header_locations if idx < i]
            if relevant_team_headers:
                current_team_for_players = relevant_team_headers[-1]

            player_no_raw = row_data.iloc[raw_col_name_to_idx.get('Player No.', -1)] if 'Player No.' in raw_col_name_to_idx else np.nan
            player_no_cleaned = np.nan
            if pd.notna(player_no_raw):
                player_no_str = str(player_no_raw).strip().lower()
                player_no_str = re.sub(r'plaeyer', 'player', player_no_str)
                match = re.match(r'(?:player\s*)?(\d+)', player_no_str)
                if match:
                    player_no_cleaned = int(match.group(1))

            if pd.notna(player_no_cleaned) and current_team_for_players is not None:
                player_record = {'Player No.': player_no_cleaned, 'Team': current_team_for_players, 'Game': sheet_name}
                all_stats_are_zero = True

                # --- Explicitly parse FG, 3PT, FTA from raw data into FGM/FGA, 3PTM/3PA, FTM/FTA ---
                
                # FG (Field Goals Made and Attempted)
                fg_val_raw = row_data.iloc[raw_col_name_to_idx.get('FG', -1)] if 'FG' in raw_col_name_to_idx else '0-0'
                fgm = 0
                fga = 0
                if pd.notna(fg_val_raw) and isinstance(fg_val_raw, str):
                    match_fg = re.match(r'(\d+)-(\d+)', str(fg_val_raw).strip())
                    if match_fg:
                        fgm = int(match_fg.group(1))
                        fga = int(match_fg.group(2))
                    else: # Handle cases like just "0" or "5" (assume attempts = made if not X-Y)
                        fgm = pd.to_numeric(str(fg_val_raw).strip(), errors='coerce')
                        fga = fgm # Assume attempts = made if only one number
                else: # Handle numeric FG values directly as FGM (assume attempts = made)
                    fgm = pd.to_numeric(fg_val_raw, errors='coerce')
                    fga = fgm
                player_record['FGM'] = int(fgm) if pd.notna(fgm) else 0
                player_record['FGA'] = int(fga) if pd.notna(fga) else 0

                # 3PT (Three-Pointers Made and Attempted)
                _3pt_val_raw = row_data.iloc[raw_col_name_to_idx.get('3PT', -1)] if '3PT' in raw_col_name_to_idx else '0-0'
                _3ptm = 0
                _3pa = 0
                if pd.notna(_3pt_val_raw) and isinstance(_3pt_val_raw, str):
                    match_3pt = re.match(r'(\d+)-(\d+)', str(_3pt_val_raw).strip())
                    if match_3pt:
                        _3ptm = int(match_3pt.group(1))
                        _3pa = int(match_3pt.group(2))
                    else:
                        _3ptm = pd.to_numeric(str(_3pt_val_raw).strip(), errors='coerce')
                        _3pa = _3ptm
                else:
                    _3ptm = pd.to_numeric(_3pt_val_raw, errors='coerce')
                    _3pa = _3ptm
                player_record['3PTM'] = int(_3ptm) if pd.notna(_3ptm) else 0
                player_record['3PA'] = int(_3pa) if pd.notna(_3pa) else 0

                # FTA (Free Throws Made and Attempted)
                fta_val_raw = row_data.iloc[raw_col_name_to_idx.get('FTA', -1)] if 'FTA' in raw_col_name_to_idx else '0-0'
                ftm = 0
                fta = 0
                if pd.notna(fta_val_raw) and isinstance(fta_val_raw, str):
                    match_ft = re.match(r'(\d+)-(\d+)', str(fta_val_raw).strip())
                    if match_ft:
                        ftm = int(match_ft.group(1))
                        fta = int(match_ft.group(2))
                    else:
                        ftm = pd.to_numeric(str(fta_val_raw).strip(), errors='coerce')
                        fta = ftm
                else:
                    ftm = pd.to_numeric(fta_val_raw, errors='coerce')
                    fta = ftm
                player_record['FTM'] = int(ftm) if pd.notna(ftm) else 0
                player_record['FTA'] = int(fta) if pd.notna(fta) else 0

                # Process other stats columns (MIN, PTS, REB, AST, STL, BLK, TOV, PF, and percentages)
                # These are direct mappings or percentage parsing from raw columns
                # Exclude 'FG', '3PT', 'FTA' from this loop as they are handled above
                cols_to_process_directly = [
                    'MIN', 'PTS', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF',
                    'FG_PCT', '3P%', 'FT%'
                ]
                
                for col_name in cols_to_process_directly:
                    if col_name in raw_col_name_to_idx: # Check if the raw column exists
                        val = row_data.iloc[raw_col_name_to_idx[col_name]]
                        numeric_val = 0

                        if pd.isna(val) or str(val).strip().lower() in ['dnp', '-', ' ']:
                            numeric_val = 0
                        else:
                            val_str = str(val).strip()
                            if col_name in ['FG_PCT', '3P%', 'FT%']:
                                if '%' in val_str:
                                    numeric_val = pd.to_numeric(val_str.replace('%', ''), errors='coerce') / 100
                                else:
                                    numeric_val = pd.to_numeric(val_str, errors='coerce')
                                numeric_val = numeric_val if pd.notna(numeric_val) else 0
                            else:
                                # Corrected: Handle scalar result from pd.to_numeric
                                temp_numeric_val = pd.to_numeric(val_str, errors='coerce')
                                numeric_val = temp_numeric_val if pd.notna(temp_numeric_val) else 0
                        player_record[col_name] = numeric_val
                    else:
                        player_record[col_name] = 0 # If raw column is missing, default to 0

                # Check if any of the *final* stats columns have non-zero values
                # We need to make sure all expected stats_cols_excel_headers are in player_record
                # and then check their sum.
                temp_player_record_df = pd.DataFrame([player_record])
                stats_cols_for_zero_check = [c for c in stats_cols_excel_headers if c in temp_player_record_df.columns]
                if not temp_player_record_df.empty and stats_cols_for_zero_check:
                    all_stats_are_zero = (temp_player_record_df[stats_cols_for_zero_check].sum(axis=1) == 0).all()
                else:
                    all_stats_are_zero = True # If no stats columns or empty, treat as all zero

                if not all_stats_are_zero:
                    processed_player_records.append(player_record)
            
        if processed_player_records:
            df_cleaned = pd.DataFrame(processed_player_records)
        else:
            # Ensure all expected columns are present even if empty
            df_cleaned = pd.DataFrame(columns=['Player No.', 'Team', 'Game'] + stats_cols_excel_headers)

        # Final type conversions and fillna for all expected stats columns
        for col in stats_cols_excel_headers:
            if col in df_cleaned.columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce').fillna(0)
            else:
                df_cleaned[col] = 0 # Add missing expected stat columns as 0

        if 'Player No.' in df_cleaned.columns:
            df_cleaned['Player No.'] = df_cleaned['Player No.'].astype(int)
        if 'Team' in df_cleaned.columns:
            df_cleaned['Team'] = df_cleaned['Team'].astype(str).str.strip().replace('nan', np.nan)
            df_cleaned.dropna(subset=['Team'], inplace=True)

        stats_cols_in_df = [col for col in stats_cols_excel_headers if col in df_cleaned.columns]
        if not df_cleaned.empty and stats_cols_in_df:
            df_cleaned = df_cleaned[df_cleaned[stats_cols_in_df].sum(axis=1) != 0].copy()
        else:
            return None

        print(f"✅ Successfully processed {sheet_name} with {len(df_cleaned)} rows.")
        print(f"🚀 Final Team Names in {sheet_name}: {sorted(df_cleaned['Team'].dropna().unique())}")
        return df_cleaned

    cleaned_dfs = {}
    for sheet_name in xls.sheet_names:
        try:
            if sheet_name in ["Acronyms", "Sheet3"]:
                print(f"Skipping non-box score sheet: '{sheet_name}'")
                continue

            if not re.match(BOX_SCORE_SHEET_PATTERN, sheet_name, re.IGNORECASE):
                print(f"Skipping unexpected sheet: '{sheet_name}' (does not match box score pattern).")
                continue

            # Extract team names from sheet name and combine with default team_labels
            match_vs = re.match(r'(\w+)\s+vs\s+(\w+)', sheet_name, re.IGNORECASE)
            sheet_team_labels = team_labels.copy()  # Start with default team labels
            if match_vs:
                team1 = match_vs.group(1).title()
                team2 = match_vs.group(2).title()
                # Add teams from sheet name if not already in the list
                if team1 not in sheet_team_labels:
                    sheet_team_labels.append(team1)
                if team2 not in sheet_team_labels:
                    sheet_team_labels.append(team2)
                print(f"📋 Processing '{sheet_name}' with teams: {team1}, {team2}")

            # Pass sheet-specific team_labels that include teams from the sheet name
            cleaned_df = clean_single_box_score_sheet(pd.read_excel(xls, sheet_name=sheet_name), sheet_name, sheet_team_labels)
            if cleaned_df is not None and not cleaned_df.empty:
                cleaned_dfs[sheet_name] = cleaned_df
            elif cleaned_df is not None and cleaned_df.empty:
                print(f"Info: Sheet '{sheet_name}' processed but resulted in an empty DataFrame after filtering.")
        except Exception as e:
            print(f"Encountered error processing sheet '{sheet_name}': {e}")
            continue

    # Print the contents of cleaned_dfs before concatenation
    print("\n--- Debugging: Contents of 'cleaned_dfs' before final concatenation ---")
    if cleaned_dfs:
        for sheet_name, df_val in cleaned_dfs.items():
            print(f"  Sheet: '{sheet_name}', Rows: {len(df_val)}, Team non-null: {df_val['Team'].notna().sum()}")
    else:
        print("  'cleaned_dfs' is empty. No sheets were successfully processed.")

    # Combine all processed sheets into one dataset
    if cleaned_dfs:
        df_combined = pd.concat([df for df in cleaned_dfs.values() if df is not None], ignore_index=True)
    else:
        df_combined = pd.DataFrame() # Ensure df_combined is an empty DataFrame if no data is processed
        print("No valid dataframes to concatenate. df_combined is empty.")

    # Validate if "Efficiency" exists before applying transformations (from original notebook's Cell 5)
    if not df_combined.empty and ("Efficiency" not in df_combined.columns or df_combined["Efficiency"].isnull().all() or df_combined["Efficiency"].empty):
        print("⚠️ 'Efficiency' column missing, empty, or needs recalculation—creating/recalculating it NOW!")
        # Use .get() with a default of 0 to ensure columns exist for calculation
        df_combined["Efficiency"] = (
            df_combined.get("PTS", 0) + df_combined.get("REB", 0) + df_combined.get("AST", 0) + 
            df_combined.get("STL", 0) + df_combined.get("BLK", 0)
        ) - (
            df_combined.get("TOV", 0) + df_combined.get("PF", 0)
        )

    # Display the first few rows of the cleaned and combined DataFrame
    print("\nCombined and Cleaned DataFrame Head:")
    if not df_combined.empty:
        print(df_combined.head())
    else:
        print("DataFrame is empty.")

    print("\nCombined and Cleaned DataFrame Info:") 
    if not df_combined.empty:
        df_combined.info()
        print("\nValue Counts for 'Team' in df_combined:")
        print(df_combined['Team'].value_counts(dropna=False))
        print("\nValue Counts for 'Player No.' in df_combined:")
        print(df_combined['Player No.'].value_counts(dropna=False))
        print("\nUnique Player-Team Combinations in df_combined:")
        print(df_combined.groupby(['Team', 'Player No.']).size().sort_index())
    else:
        print("DataFrame is empty.")

    # Save cleaned dataset
    if not df_combined.empty:
        df_combined.to_csv(cleaned_data_file, index=False)
        print(f"✅ {cleaned_data_file} saved.")
    else:
        print(f"No data to save to {cleaned_data_file} as df_combined is empty.")

    print("--- ETL Process Complete ---")

# 📌 Cell 4: Data Validation and Final Touches
def validate_and_finalize_data():
    global df_combined # Declare global to modify df_combined
    print("\n--- Data Validation and Final Touches ---")

    if df_combined.empty:
        print("df_combined is empty. Skipping data validation and final touches.")
    else:
        # Ensure 'MIN' column is numeric and handle potential 'DNP' values (if not already done in Cell 3)
        if 'MIN' in df_combined.columns:
            df_combined['MIN'] = pd.to_numeric(df_combined['MIN'], errors='coerce').fillna(0)

        # Fill any remaining NaN values in relevant statistical columns with 0
        for col in stats_cols_excel_headers:
            if col in df_combined.columns:
                df_combined[col] = df_combined[col].fillna(0)

        # Drop any rows that might have become entirely non-numeric or irrelevant after cleaning
        # (e.g., if Player No. or Team somehow became NaN unexpectedly)
        if 'Player No.' in df_combined.columns:
            df_combined = df_combined[df_combined['Player No.'].notna()].copy()

            # Also drop rows where all statistical columns are zero (summary rows or non-players that slipped through)
            stats_cols_to_check = [col for col in stats_cols_excel_headers if col in df_combined.columns]
            if stats_cols_to_check:
                df_combined = df_combined[df_combined[stats_cols_to_check].sum(axis=1) != 0].copy()
            else:
                print("Warning: No statistical columns found for final filtering based on sum.")
        else:
            print("Warning: 'Player No.' column not found in df_combined, skipping player-based row drop.")


    print("✅ Final Data Validation Complete.")
    print("Updated Combined DataFrame Info:")
    if not df_combined.empty:
        df_combined.info()
    else:
        print("DataFrame is empty after validation.")

# 📌 Cell 5: Summary Statistics
def generate_summary_statistics():
    print("\n--- Generating Summary Statistics ---")

    if df_combined.empty:
        print("df_combined is empty. Skipping summary statistics generation.")
    else:
        # Basic descriptive statistics for numeric columns
        print("\n🚀 Descriptive Statistics for Numeric Columns:")
        print(df_combined.describe())

        # Group by team and player to see average stats
        print("\n📊 Average Statistics per Player (across all games):")
        # Exclude non-numeric and identifying columns from mean calculation
        numeric_cols_for_mean = [col for col in stats_cols_excel_headers + ['Efficiency', 'TS%', 'Weighted_TS%'] if col in df_combined.columns]


        # Calculate average for each player
        df_player_avg = df_combined.groupby(['Player No.', 'Team'])[numeric_cols_for_mean].mean().reset_index()
        print(df_player_avg.sort_values(by="PTS", ascending=False).head(10)) # Top 10 players by average points

        print("\n🏀 Team-wise Aggregated Statistics (Average per game, per player):")
        df_team_avg = df_combined.groupby('Team')[numeric_cols_for_mean].mean().reset_index()
        print(df_team_avg.sort_values(by='PTS', ascending=False)) # Teams by average points

        print("\n--- Summary Statistics Generation Complete ---")

# 📌 Cell 6: Player-Team Label & Visual Data Preparation
def prepare_visual_data():
    global df_visual, df_combined # df_combined is modified here (Player_Team_Label added), df_visual is created here
    print("\n--- Preparing Data for Visualizations ---")

    if df_combined.empty:
        print("df_combined is empty. Skipping visualization data preparation.")
    else:
        # Create a unique identifier for each player that includes their team
        # This helps distinguish between players with the same 'Player No.' but different teams
        df_combined['Player_Team_Label'] = df_combined.apply(
            lambda row: f"Player {row['Player No.']} ({row['Team']})" if pd.notna(row['Team']) else str(row['Player No.']),
            axis=1
        )
        print("\n--- Debug: After Player_Team_Label creation in prepare_visual_data ---")
        print(f"Columns in df_combined: {df_combined.columns.tolist()}")
        print(f"df_combined head with Player_Team_Label:\n{df_combined[['Player No.', 'Team', 'Player_Team_Label']].head()}")


        # Ensure 'Game' column is categorical for consistent plotting order if needed
        df_combined['Game'] = df_combined['Game'].astype('category')

        # Create a DataFrame specifically for visualizations (should have all desired columns from df_combined)
        df_visual = df_combined.copy()

        print("\n🚀 First 5 Rows of df_visual with Player_Team_Label:")
        print(df_visual[['Player No.', 'Team', 'Game', 'PTS', 'REB', 'AST', 'Player_Team_Label', 'Efficiency', 'MIN', 'TS%']].head()) # Include some advanced stats to verify
        print("\nDebug: df_visual info after Player_Team_Label creation and potential advanced stats:")
        df_visual.info()

        print("\n✅ Data for Visualizations Prepared.")

# 📌 Cell 7: Advanced Visualizations
def generate_advanced_visualizations():
    global df_visual
    print("\n--- Generating Advanced Visualizations ---")

    if df_visual.empty:
        print("df_visual is empty. Skipping advanced visualization generation.")
    else:

        # Ensure required columns for plots exist
        required_plot_cols = ['PTS', 'REB', 'AST', 'Efficiency', 'Player_Team_Label', 'Game', 'FG_PCT', 'TOV', 'FGA', 'TS%']
        if not all(col in df_visual.columns for col in required_plot_cols):
            print(f"⚠️ Missing one or more required columns for plotting: {required_plot_cols}. Skipping advanced visualizations.")
        else:
            # Set a consistent style for plots
            sns.set_style("whitegrid")
            plt.rcParams["figure.figsize"] = (10, 6) # Default figure size

            # 🔥 **Box Plot: Efficiency by Team**
            if 'Team' in df_visual.columns and df_visual['Team'].notna().any():
                plt.figure(figsize=(10, 6))
                # Use a categorical palette, ensuring we have enough colors for all unique teams
                sns.boxplot(x='Team', y='Efficiency', data=df_visual, palette='tab10') 
                plt.title('Player Efficiency Distribution by Team')
                plt.xlabel('Team')
                plt.ylabel('Efficiency Rating')
                plt.tight_layout()
                plt.savefig('Efficiency_by_Team_BoxPlot.png')
                plt.close() # Close the figure
                print("✅ Efficiency by Team Box Plot saved as Efficiency_by_Team_BoxPlot.png")
            else:
                print("Skipping Efficiency by Team Box Plot: 'Team' column is missing or empty.")

            # 🔥 **Scatter Plot: Points vs. Efficiency (Individual Player Highlights)**
            plt.figure(figsize=(12, 8))
            sns.scatterplot(x='PTS', y='Efficiency', hue='Player_Team_Label', data=df_visual, s=100, alpha=0.7)
            plt.title('Points vs. Efficiency for All Players')
            plt.xlabel('Points (PTS)')
            plt.ylabel('Efficiency Rating')
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
            plt.tight_layout()
            plt.savefig('Points_vs_Efficiency_Scatter.png')
            plt.close() # Close the figure
            print("✅ Points vs. Efficiency Scatter plot saved as Points_vs_Efficiency_Scatter.png")

            # 🔥 **Line Plot: Player Progress (Points per game)** - Select top 5 players by total points
            # Only plot if there's actual game data (more than one game per player)
            if df_visual['Game'].nunique() > 1:
                player_total_points = df_visual.groupby('Player_Team_Label')['PTS'].sum().nlargest(5).index
                
                plt.figure(figsize=(14, 7))
                for player in player_total_points:
                    df_player = df_visual[df_visual['Player_Team_Label'] == player].sort_values('Game')
                    plt.plot(df_player['Game'], df_player['PTS'], marker='o', label=player)

                plt.title('Top 5 Players: Points Per Game Progress Over Games')
                plt.xlabel('Game')
                plt.ylabel('Points')
                plt.xticks(rotation=45, ha='right')
                plt.legend(title='Player (Team)', bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.grid(True, linestyle='--', alpha=0.6)
                plt.tight_layout()
                plt.savefig('Top_Players_Points_Progress.png')
                plt.close() # Close the figure
                print("✅ Top 5 Players: Points Per Game Progress plot saved as Top_Players_Points_Progress.png")
            else:
                print("Skipping Player Progress plot: Not enough games to show progress (only one game detected).")


            # 🔥 **Bar Chart: Shooting Percentage Comparison (Top N players)**
            # Calculate average shooting percentage for each player
            df_avg_shooting = df_visual.groupby('Player_Team_Label')['FG_PCT'].mean().sort_values(ascending=False).head(10)

            if not df_avg_shooting.empty:
                plt.figure(figsize=(12, 6))
                df_avg_shooting.plot(kind='bar', color='skyblue')
                
                plt.title('Top 10 Players by Average Field Goal Percentage')
                plt.xlabel('Player (Team)')
                plt.ylabel('Field Goal %')
                plt.xticks(rotation=45, ha='right')
                plt.legend(title="Team", bbox_to_anchor=(1.05, 1), loc='upper left') # Added legend
                plt.tight_layout()
                plt.savefig('Top_Players_Shooting_Percentage.png')
                plt.close() # Close the figure
                print("✅ Top 10 Players by Average Field Goal Percentage plot saved as Top_Players_Shooting_Percentage.png")
            else:
                print("No valid data for Top 10 Players by Average Field Goal Percentage plot.")


            # 🔥 **Scatter Plot: Assist vs Turnover Ratio (Player-wise)**
            # Calculate average assists and turnovers per player
            df_avg_ast_tov = df_visual.groupby('Player_Team_Label')[['AST', 'TOV']].mean().reset_index()

            if not df_avg_ast_tov.empty:
                plt.figure(figsize=(10, 7))
                sns.scatterplot(x='AST', y='TOV', hue='Player_Team_Label', data=df_avg_ast_tov, s=150, alpha=0.8)
                
                plt.title('Average Assists vs. Turnovers Per Player')
                plt.xlabel('Average Assists Per Game')
                plt.ylabel('Average Turnovers Per Game')
                plt.grid(True, linestyle='--', alpha=0.6)
                plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
                plt.tight_layout()
                plt.savefig('Assists_vs_Turnovers_Scatter.png')
                plt.close() # Close the figure
                print("✅ Average Assists vs. Turnovers Scatter plot saved as Assists_vs_Turnovers_Scatter.png")
            else:
                print("No valid data for Average Assists vs. Turnovers plot.")

            # 🔥 **Bar Chart: Top 10 Players by Average Efficiency**
            # Group by 'Player_Team_Label' to calculate average efficiency
            df_avg_efficiency_plot = df_visual.groupby('Player_Team_Label')['Efficiency'].mean().sort_values(ascending=False).head(10).reset_index()

            # Ensure we have Player No. and Team for hue if needed, or just plot by Player_Team_Label directly
            # For this plot, directly using Player_Team_Label on x-axis is common if not distinguishing by hue for team
            if not df_avg_efficiency_plot.empty:
                plt.figure(figsize=(12, 6))
                sns.barplot(x='Player_Team_Label', y='Efficiency', data=df_avg_efficiency_plot, palette='viridis')
                
                plt.title('Top 10 Players by Average Efficiency Rating')
                plt.xlabel('Player (Team)')
                plt.ylabel('Efficiency Rating')
                plt.xticks(rotation=45, ha='right')
                # plt.legend(title="Team", bbox_to_anchor=(1.05, 1), loc='upper left') # No hue, no legend needed here
                plt.tight_layout()
                plt.savefig('Top_Players_Efficiency.png')
                plt.close() # Close the figure
                print("✅ Top 10 Players by Average Efficiency plot saved as Top_Players_Efficiency.png")
            else:
                print("No valid data for Top 10 Players by Average Efficiency plot.")

            # Step 1: Compute player-level total FGA and avg TS%
            player_ts_stats = (
                df_visual.groupby('Player_Team_Label')
                .agg(
                    Total_FGA=('FGA', 'sum'),
                    Avg_TS_Pct=('TS%', 'mean')
                )
                .reset_index()
            )

            # Step 2: Filter players with at least 5 total FGA
            eligible_ts = player_ts_stats[player_ts_stats['Total_FGA'] >= 5].copy()

            # Step 3: Take top 10 by average TS%
            df_top_ts = eligible_ts.sort_values(by='Avg_TS_Pct', ascending=False).head(10)

            # Step 4: Print for debugging
            print("\n🏀 Top 10 Players by Avg True Shooting % (min 5 total FGA):")
            print(df_top_ts)

            # Step 5: Plot
            if not df_top_ts.empty:
                plt.figure(figsize=(12, 6))
                sns.barplot(x='Player_Team_Label', y='Avg_TS_Pct', data=df_top_ts, palette='magma')
                plt.title('Top 10 Players by Avg True Shooting % (≥ 5 Total FGA)')
                plt.xlabel('Player (Team)')
                plt.ylabel('Average TS%')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                plt.savefig('Top10_TS_Percentage_Filtered.png')
                plt.close()
                print("✅ Final TS% chart saved as Top10_TS_Percentage_Filtered.png")
            else:
                print("No players met the criteria for TS% visualization.")

    print("\n--- Advanced Visualizations Generation Complete ---")


# 📌 Cell 8: Final Visualizations (Histograms)
def generate_final_visualizations(): 
    print("\n--- Generating Final Visualizations (Histograms) ---")

    # Define columns for which to generate histograms
    histogram_cols = ['PTS', 'REB', 'AST', 'FG_PCT', '3P%', 'FT%', 'TOV']


    # Loop through columns and generate histograms if data exists
    if df_visual.empty:
        print("Skipping visualizations due to empty df_visual DataFrame.")
    else:
        for col in histogram_cols:
            if col in df_visual.columns and not df_visual[col].empty and df_visual[col].sum() > 0:
                print(f"\nGenerating histogram for {col} distribution...")
                plt.figure(figsize=(10, 6))
                sns.histplot(df_visual[col], kde=True, bins=10, color='skyblue')
                plt.title(f'{col} Distribution Across All Players')
                plt.xlabel(col)
                plt.ylabel('Number of Instances')
                plt.tight_layout()
                filename = f'{col}_Distribution_Cleaned.png'
                plt.savefig(filename)
                plt.close() # Close the figure
                print(f"✅ {col} distribution histogram saved as {filename}")
            else:
                print(f"⚠️ Skipping histogram for {col} due to missing column, empty data, or all zero values.")

        # 🔥 Bar plot for Top 10 Players by Average Efficiency with dodge=True (uses Player No. and Team for hue)
        print("\nGenerating Top 10 Players by Average Efficiency plot (with team distinction)...")
        try:
            # Check if player_advanced_stats_avg is already defined and not empty, otherwise try to load/re-calculate
            if 'player_advanced_stats_avg' not in globals() or player_advanced_stats_avg.empty:
                print("Debug: player_advanced_stats_avg not found/empty for final visualizations, attempting to load from CSV.")
                try:
                    player_advanced_stats_avg = pd.read_csv(advanced_stats_file)
                    print(f"Debug: Loaded player_advanced_stats_avg from {advanced_stats_file}.")
                except FileNotFoundError:
                    print(f"⚠️ {advanced_stats_file} not found for final visualizations. Skipping this plot.")
                    player_advanced_stats_avg = pd.DataFrame() # Ensure it's empty to prevent further errors

            if not player_advanced_stats_avg.empty and 'Player No.' in player_advanced_stats_avg.columns and 'Team' in player_advanced_stats_avg.columns and 'Avg_Efficiency' in player_advanced_stats_avg.columns:
                top_efficiency_players = player_advanced_stats_avg.sort_values(by='Avg_Efficiency', ascending=False).head(10).copy()

                # Print the DataFrame used for the plot
                print("\n📊 Data for Top 10 Players by Average Efficiency (Team-Specific) plot:")
                print(top_efficiency_players)

                if not top_efficiency_players.empty:
                    plt.figure(figsize=(12, 7))
                    sns.barplot(x='Player No.', y='Avg_Efficiency', hue='Team', data=top_efficiency_players, palette='coolwarm', dodge=True) # Changed dodge to True
                    plt.title('Top 10 Players by Average Efficiency (Team-Specific)')
                    plt.ylabel('Average Efficiency Rating')
                    plt.xlabel('Player Number')
                    plt.xticks(rotation=45, ha='right')
                    plt.legend(title="Team", bbox_to_anchor=(1.05, 1), loc='upper left')
                    plt.tight_layout()
                    plt.savefig("Top_Players_Efficiency_Team_Specific.png")
                    plt.close() # Close the figure
                    print("✅ Top 10 Players by Average Efficiency (Team-Specific) plot saved as Top_Players_Efficiency_Team_Specific.png")
                else:
                    print("No valid data for Top 10 Players by Average Efficiency plot.")
            else:
                print("Skipping Top 10 Players by Average Efficiency plot: Missing expected columns or data is empty in player_advanced_stats_avg.")
        except KeyError as e:
            print(f"Skipping Top 10 Players by Average Efficiency plot: Missing expected column for plotting. Error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while generating Top 10 Players by Average Efficiency plot: {e}")


    print("\n--- Final Visualizations Generation Complete ---")

# 📌 Cell 9: Calculation of Advanced Player Statistics (Including WS & VORP Proxies)
def calculate_advanced_stats(): 
    global df_combined # Declare global to modify df_combined
    print("\n--- Calculating Advanced Player Statistics (Including WS & VORP Proxies) ---")

    if df_combined.empty:
        print("df_combined is empty. Skipping advanced statistics calculation.")
    else:
        # Create a copy to add new calculated columns without modifying the original df_combined during intermediate steps
        df_advanced_stats = df_combined.copy()

        # --- Per-Minute Statistics ---
        # Handle division by zero for minutes: if MIN is 0, these stats are 0 or NaN.
        # We'll use 0 as the result for DNP players (0 minutes, 0 stats).
        # Create a temporary 'MIN_adjusted' column to avoid division by zero errors for calculations
        df_advanced_stats['MIN_adjusted'] = df_advanced_stats['MIN'].replace(0, np.nan)

        for stat in ['PTS', 'REB', 'AST', 'STL', 'BLK']:
            df_advanced_stats[f'{stat}_Per_Min'] = (df_advanced_stats[stat] / df_advanced_stats['MIN_adjusted']).fillna(0)
        
        print("✅ Per-Minute statistics calculated.")

        # --- Assist-to-Turnover Ratio (AST/TOV) ---
        print("🧮 Calculating Assist-to-Turnover Ratio...")

        # Step 1: Compute ratio while avoiding division by zero
        df_advanced_stats['AST_TOV_Ratio'] = df_advanced_stats['AST'] / df_advanced_stats['TOV'].replace(0, np.nan)

        # Step 2: Assign a very high ratio for AST > 0 and TOV == 0
        df_advanced_stats.loc[
            (df_advanced_stats['AST'] > 0) & (df_advanced_stats['TOV'] == 0),
            'AST_TOV_Ratio'
        ] = 999

        # Step 3: Fill NaNs (e.g., AST and TOV both 0) with 0
        df_advanced_stats['AST_TOV_Ratio'] = df_advanced_stats['AST_TOV_Ratio'].fillna(0)

        print("✅ Assist-to-Turnover Ratio calculated.")


        # --- True Shooting Percentage (TS%) ---
        # Now that FGM, FGA, 3PTM, 3PA, FTM, FTA are correctly parsed, use them directly.
        print("🧮 Calculating True Shooting Percentage (TS%)...")

        # Calculate Total Shot Attempts for TS% denominator: FGA + 0.44 * FTA
        # Ensure FGA and FTA are numeric, fillna(0)
        df_advanced_stats['FGA'] = pd.to_numeric(df_advanced_stats['FGA'], errors='coerce').fillna(0)
        df_advanced_stats['FTA'] = pd.to_numeric(df_advanced_stats['FTA'], errors='coerce').fillna(0)
        df_advanced_stats['PTS'] = pd.to_numeric(df_advanced_stats['PTS'], errors='coerce').fillna(0)

        df_advanced_stats['TS_Denominator'] = df_advanced_stats['FGA'] + 0.44 * df_advanced_stats['FTA']

        # Calculate TS% (handling division by zero for denominator)
        df_advanced_stats['TS%'] = (df_advanced_stats['PTS'] / (2 * df_advanced_stats['TS_Denominator'].replace(0, np.nan))).fillna(0) * 100
        df_advanced_stats['TS%'] = df_advanced_stats['TS%'].replace(np.inf, 0) # Inf for 0 pts, 0 attempts -> 0 for practical use
        print("✅ True Shooting Percentage (TS%) calculated.")


        # --- Rebounding Rates (Player Total Rebounding Percentage - REB_Percentage) ---
        # Calculate total game rebounds for each game to use as denominator
        game_total_rebounds = df_advanced_stats.groupby('Game')['REB'].sum().reset_index()
        game_total_rebounds.rename(columns={'REB': 'Total_Game_REB'}, inplace=True)
        df_advanced_stats = pd.merge(df_advanced_stats, game_total_rebounds, on='Game', how='left')

        # Player REB% = (Player REB / Total Game REB) * 100
        df_advanced_stats['REB_Percentage'] = (df_advanced_stats['REB'] / df_advanced_stats['Total_Game_REB'].replace(0, np.nan)).fillna(0) * 100
        print("✅ Player Total Rebounding Percentage calculated.")

        # --- Simplified Win Shares (WS_Simplified) ---
        # This is a highly simplified proxy. Official WS is very complex.
        # Proxy: A combination of Efficiency, True Shooting %, and Rebounding %
        df_advanced_stats['WS_Simplified'] = (
            (df_advanced_stats['Efficiency'] * 0.1) +      # Efficiency is a strong indicator
            (df_advanced_stats['TS%'] * 0.005) +           # Contribution from efficient scoring
            (df_advanced_stats['REB_Percentage'] * 0.002)  # Contribution from rebounding
        ).fillna(0)
        df_advanced_stats.loc[df_advanced_stats['WS_Simplified'] < 0, 'WS_Simplified'] = 0 # No negative WS
        print("✅ Simplified Win Shares (WS_Simplified) calculated (Proxy).")
        print("   Note: This is a simplified proxy, not the official NBA Win Shares calculation.")

        # --- Simplified Value Over Replacement Player (VORP_Simplified) ---
        # This is also a highly simplified proxy. Official VORP is based on Box Plus/Minus.
        # Proxy: Scaled version of simplified WS, or tied to efficiency relative to an average.
        # Let's make it a scaled version of simplified WS.
        df_advanced_stats['VORP_Simplified'] = (df_advanced_stats['WS_Simplified'] * 0.05).fillna(0)
        print("✅ Simplified Value Over Replacement Player (VORP_Simplified) calculated (Proxy).")
        print("   Note: This is a simplified proxy, not the official NBA VORP calculation.")


        # --- Usage Rate (USG%) ---
        print("\n❌ Usage Rate (USG%) cannot be accurately calculated with available box score data.")
        print("   USG% typically requires knowing total team possessions and individual field goal attempts (FGA),")
        print("   free throw attempts (FTA), and turnovers (TOV) relative to those possessions for accurate calculation.")
        print("   This dataset lacks explicit team possession counts and distinct opponent team FGA/FTA.")
        print("   A basic proxy could be made (e.g., Player PTS / Team PTS), but would be a significant oversimplification.")

        # Drop temporary columns used for calculations
        df_advanced_stats.drop(columns=[
            'MIN_adjusted', 'TS_Denominator', 'Total_Game_REB'
        ], inplace=True, errors='ignore') # Removed FG_PCT_adjusted_for_div, 3P%_adjusted_for_div, FGA_inferred, 3PA_inferred as they are no longer needed

        # Update df_combined with the new advanced stats
        # Crucially, Player_Team_Label *should* be present in df_combined already from prepare_visual_data,
        # so this copy should retain it.
        df_combined = df_advanced_stats.copy()

        print("\n🚀 First 5 Rows of df_combined with new advanced stats:")
        print(df_combined[['Player No.', 'Team', 'PTS', 'Efficiency', 'PTS_Per_Min', 'AST_TOV_Ratio', 'TS%', 'REB_Percentage', 'WS_Simplified', 'VORP_Simplified']].head())

        print("\nUpdated df_combined Info:")
        print(df_combined.info())

    print("--- Advanced Statistics Calculation Complete ---")


# 📌 Cell 10: Advanced Summary Statistics & Table Presentation (Including WS & VORP)
def generate_advanced_summary_statistics(): 
    global player_advanced_stats_avg # Declare global for modification
    print("\n--- Generating Advanced Summary Statistics ---")

    if df_combined.empty:
        print("df_combined is empty. Skipping advanced summary statistics.")
    else:
        # --- Debug: Check for Player_Team_Label before grouping ---
        print("\n--- Debug: Before grouping by Player_Team_Label in generate_advanced_summary_statistics ---")
        print(f"Columns in df_combined: {df_combined.columns.tolist()}")
        if 'Player_Team_Label' in df_combined.columns:
            print(f"Player_Team_Label exists. Non-null count: {df_combined['Player_Team_Label'].notna().sum()}")
            print(f"Unique Player_Team_Label values: {df_combined['Player_Team_Label'].dropna().unique()}")
        else:
            print("Player_Team_Label does NOT exist in df_combined at this point.")
            return # Exit if the column is truly missing to avoid KeyError

        # --- Player-level Aggregation of Advanced Stats ---
        # Aggregate by Player_Team_Label to get average advanced stats across all games for each player
        player_advanced_stats_avg = df_combined.groupby('Player_Team_Label').agg(
            Avg_PTS_Per_Min=('PTS_Per_Min', 'mean'),
            Avg_REB_Per_Min=('REB_Per_Min', 'mean'),
            Avg_AST_Per_Min=('AST_Per_Min', 'mean'),
            Avg_STL_Per_Min=('STL_Per_Min', 'mean'),
            Avg_BLK_Per_Min=('BLK_Per_Min', 'mean'),
            Avg_AST_TOV_Ratio=('AST_TOV_Ratio', 'mean'),
            Avg_TS_Percentage=('TS%', 'mean'),
            Avg_REB_Percentage=('REB_Percentage', 'mean'),
            Avg_Efficiency=('Efficiency', 'mean'),
            Avg_WS_Simplified=('WS_Simplified', 'mean'),
            Avg_VORP_Simplified=('VORP_Simplified', 'mean'),
            Total_Games_Played=('Game', 'nunique'),
            Total_MIN_Played=('MIN', 'sum'),
            Total_FGA=('FGA', 'sum')  # ✅ Add this line
        ).reset_index()



        # Extract Player No. and Team for better readability in tables
        player_advanced_stats_avg['Player No.'] = player_advanced_stats_avg['Player_Team_Label'].apply(
            lambda x: int(re.search(r'Player (\d+)', x).group(1)) if re.search(r'Player (\d+)', x) else np.nan
        )
        player_advanced_stats_avg['Team'] = player_advanced_stats_avg['Player_Team_Label'].apply(
            lambda x: re.search(r'\((.*?)\)', x).group(1) if re.search(r'\((.*?)\)', x) else 'Unknown'
        )
        # Reorder columns for better presentation, ENSURING Player_Team_Label is kept
        player_advanced_stats_avg = player_advanced_stats_avg[['Player No.', 'Team', 'Player_Team_Label', 'Total_Games_Played', 'Total_MIN_Played', # ADDED 'Player_Team_Label'
                                                               'Avg_PTS_Per_Min', 'Avg_REB_Per_Min', 'Avg_AST_Per_Min',
                                                               'Avg_STL_Per_Min', 'Avg_BLK_Per_Min', 'Avg_AST_TOV_Ratio',
                                                               'Avg_TS_Percentage', 'Avg_REB_Percentage', 'Avg_Efficiency',
                                                               'Avg_WS_Simplified', 'Avg_VORP_Simplified']].copy() # Added Avg_Efficiency, WS, VORP

        print("\n--- Top 10 Players by Average True Shooting Percentage (TS%) ---")
        print(player_advanced_stats_avg.sort_values(by='Avg_TS_Percentage', ascending=False).head(10))

        print("\n--- Top 10 Players by Average Points Per Minute (PPM) ---")
        print(player_advanced_stats_avg.sort_values(by='Avg_PTS_Per_Min', ascending=False).head(10))

        print("\n--- Top 10 Players by Average Assist-to-Turnover Ratio ---")
        print(player_advanced_stats_avg.sort_values(by='Avg_AST_TOV_Ratio', ascending=False).head(10))

        print("\n--- Top 10 Players by Average Rebounding Percentage ---")
        print(player_advanced_stats_avg.sort_values(by='Avg_REB_Percentage', ascending=False).head(10))

        print("\n--- Top 10 Players by Average Simplified Win Shares (WS_Simplified) ---")
        print(player_advanced_stats_avg.sort_values(by='Avg_WS_Simplified', ascending=False).head(10))

        print("\n--- Top 10 Players by Average Simplified VORP (VORP_Simplified) ---")
        print(player_advanced_stats_avg.sort_values(by='Avg_VORP_Simplified', ascending=False).head(10))


        # --- Team-level Aggregation of Advanced Stats ---
        team_advanced_stats_avg = df_combined.groupby('Team').agg(
            Avg_Team_Efficiency=('Efficiency', 'mean'), # Average efficiency per player on the team per game
            Avg_Team_TS_Percentage=('TS%', 'mean'),
            Avg_Team_REB_Percentage=('REB_Percentage', 'mean'),
            Avg_Team_AST_TOV_Ratio=('AST_TOV_Ratio', 'mean'),
            Avg_Team_WS_Simplified=('WS_Simplified', 'mean'), # Added WS_Simplified to team stats
            Avg_Team_VORP_Simplified=('VORP_Simplified', 'mean'), # Added VORP_Simplified to team stats
            Total_Games_Played=('Game', 'nunique')
        ).reset_index()

        print("\n--- Team-level Advanced Statistics Averages ---")
        print(team_advanced_stats_avg.sort_values(by='Avg_Team_Efficiency', ascending=False))

        # Save player_advanced_stats_avg for potential use in Cell 11 for independent execution
        player_advanced_stats_avg.to_csv(advanced_stats_file, index=False)
        print(f"✅ Player advanced statistics saved to {advanced_stats_file}")


    print("--- Advanced Summary Statistics Generation Complete ---")

# 📌 Cell 11: New Visualizations for Advanced Stats (Including WS & VORP)
def generate_new_visualizations(): 
    global player_advanced_stats_avg # Declare global for modification if needed
    print("\n--- Generating New Visualizations for Advanced Stats ---")

    if df_combined.empty: # Check df_combined as base for all operations
        print("df_combined is empty. Skipping new visualizations.")
    else:
        sns.set_style("whitegrid")
        plt.rcParams["figure.figsize"] = (12, 7)

        # To make this cell runnable independently, load player_advanced_stats_avg if not already defined
        try:
            # Check if player_advanced_stats_avg is already defined and not empty, otherwise try to load/re-calculate
            if 'player_advanced_stats_avg' not in globals() or player_advanced_stats_avg.empty: # Check globals()
                raise NameError("player_advanced_stats_avg is not defined or empty, attempting to load/re-calculate.")
        except NameError:
            print("\n--- Debug: Re-calculating player_advanced_stats_avg for visualizations (in generate_new_visualizations) ---")
            try:
                player_advanced_stats_avg = pd.read_csv(advanced_stats_file)
                print(f"Loaded player_advanced_stats_avg from {advanced_stats_file}.")
            except FileNotFoundError:
                print(f"⚠️ {advanced_stats_file} not found for final visualizations. Re-calculating.")
                # Recalculate if Cell 10 wasn't run or file not found
                if not df_combined.empty:
                    # --- Debug: Check for Player_Team_Label before re-grouping ---
                    print("\n--- Debug: Before re-grouping by Player_Team_Label for re-calculation ---")
                    print(f"Columns in df_combined: {df_combined.columns.tolist()}")
                    if 'Player_Team_Label' in df_combined.columns:
                        print(f"Player_Team_Label exists. Non-null count: {df_combined['Player_Team_Label'].notna().sum()}")
                        print(f"Unique Player_Team_Label values: {df_combined['Player_Team_Label'].dropna().unique()}")
                    else:
                        print("Player_Team_Label does NOT exist in df_combined during re-calculation. This will cause an error.")
                        player_advanced_stats_avg = pd.DataFrame() # Ensure empty to prevent further errors
                        return # Exit function if column is missing

                    player_advanced_stats_avg = df_combined.groupby('Player_Team_Label').agg(
                        Avg_PTS_Per_Min=('PTS_Per_Min', 'mean'),
                        Avg_REB_Per_Min=('REB_Per_Min', 'mean'),
                        Avg_AST_Per_Min=('AST_Per_Min', 'mean'), 
                        Avg_STL_Per_Min=('STL_Per_Min', 'mean'), # Corrected: BLK_BLK to BLK_Per_Min
                        Avg_BLK_Per_Min=('BLK_Per_Min', 'mean'), # Corrected: BLK_BLK to BLK_Per_Min
                        Avg_AST_TOV_Ratio=('AST_TOV_Ratio', 'mean'),
                        Avg_TS_Percentage=('TS%', 'mean'),
                        Avg_REB_Percentage=('REB_Percentage', 'mean'),
                        Avg_Efficiency=('Efficiency', 'mean'),
                        Avg_WS_Simplified=('WS_Simplified', 'mean'), # Added for recalculation
                        Avg_VORP_Simplified=('VORP_Simplified', 'mean'), # Added for recalculation
                        Total_Games_Played=('Game', 'nunique'),
                        Total_MIN_Played=('MIN', 'sum')
                    ).reset_index()
                    player_advanced_stats_avg['Player No.'] = player_advanced_stats_avg['Player_Team_Label'].apply(
                        lambda x: int(re.search(r'Player (\d+)', x).group(1)) if re.search(r'Player (\d+)', x) else np.nan
                    )
                    player_advanced_stats_avg['Team'] = player_advanced_stats_avg['Player_Team_Label'].apply(
                        lambda x: re.search(r'\((.*?)\)', x).group(1) if re.search(r'\((.*?)\)', x) else 'Unknown'
                    )
                else:
                    print("Cannot re-calculate player_advanced_stats_avg as df_combined is empty.")
                    player_advanced_stats_avg = pd.DataFrame() # Ensure it's empty if df_combined is empty

        if player_advanced_stats_avg.empty:
            print("No aggregated player data to generate advanced visualizations.")
        else:
            # ✅ Filter out players with fewer than 5 total FGA
            # This assumes you have a 'FGA' column in df_combined for the sum
            if 'FGA' in df_combined.columns:
                total_fga = df_combined.groupby('Player_Team_Label')['FGA'].sum()
                eligible_players = total_fga[total_fga >= 5].index
                player_advanced_stats_avg = player_advanced_stats_avg[
                player_advanced_stats_avg['Player_Team_Label'].isin(eligible_players)
                ].copy()
            else:
                print("⚠️ Skipping FGA filter in player_advanced_stats_avg: 'FGA' column not found in df_combined.")
            # --- Bar Chart: Top 10 Players by Average True Shooting Percentage (TS%) ---
            print("\nGenerating Top 10 Players by Average TS% plot...")
            top_ts_players = player_advanced_stats_avg.sort_values(by='Avg_TS_Percentage', ascending=False).head(10)
            
            if not top_ts_players.empty:
                plt.figure(figsize=(12, 7))
                sns.barplot(x='Player No.', y='Avg_TS_Percentage', hue='Team', data=top_ts_players, palette='coolwarm', dodge=True)
                plt.title('Top 10 Players by Average True Shooting Percentage')
                plt.ylabel('Average TS%')
                plt.xlabel('Player Number')
                plt.xticks(rotation=45, ha='right')
                plt.legend(title="Team", bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
                plt.savefig("Top_Players_TS_Percentage.png")
                plt.close() # Close the figure
                print("✅ Top 10 Players by Average True Shooting Percentage plot saved as Top_Players_TS_Percentage.png")
            else:
                print("No valid data for Top 10 Players by Average True Shooting Percentage plot.")

            # --- Scatter Plot: Points Per Minute (PPM) vs. Efficiency ---
            print("\nGenerating PPM vs. Efficiency plot...")
            # Ensure we filter out players with 0 minutes if PPM is used as x-axis
            players_with_ppm_and_efficiency = player_advanced_stats_avg[(player_advanced_stats_avg['Avg_PTS_Per_Min'] > 0) & 
                                                                        (player_advanced_stats_avg['Avg_Efficiency'].notna())].copy()
            
            if not players_with_ppm_and_efficiency.empty:
                plt.figure(figsize=(12, 8))
                sns.scatterplot(x='Avg_PTS_Per_Min', y='Avg_Efficiency', hue='Team', data=players_with_ppm_and_efficiency, s=150, alpha=0.8, palette='viridis')
                plt.title('Average Points Per Minute vs. Average Efficiency')
                plt.xlabel('Average Points Per Minute (PPM)')
                plt.ylabel('Average Efficiency Rating')
                plt.grid(True, linestyle='--', alpha=0.6) # Corrected: use plt.grid instead of ax_ppm_eff.grid
                plt.legend(title="Team", bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
                plt.savefig("PPM_vs_Efficiency_Scatter.png")
                plt.close() # Close the figure
                print("✅ PPM vs. Efficiency Scatter plot saved as PPM_vs_Efficiency_Scatter.png")
            else:
                print("No valid data for PPM vs. Efficiency plot (filtered out 0 minutes or missing efficiency).")

            # --- Bar Chart: Top 10 Players by Average Assist-to-Turnover Ratio ---
            print("\nGenerating Top 10 Players by Average AST/TOV Ratio plot...") 
            # Filter out players with 0 assists and 0 turnovers that might have an extremely high or 0 ratio
            # Focus on players with at least some assists or a meaningful ratio
            players_with_ast_tov = player_advanced_stats_avg[player_advanced_stats_avg['Avg_AST_TOV_Ratio'].notna() &
                                                          (player_advanced_stats_avg['Avg_AST_TOV_Ratio'] != 0)].copy() # Exclude 0/0 cases
            top_ast_tov_players = players_with_ast_tov.sort_values(by='Avg_AST_TOV_Ratio', ascending=False).head(10)

            if not top_ast_tov_players.empty:
                plt.figure(figsize=(12, 7))
                sns.barplot(x='Player No.', y='Avg_AST_TOV_Ratio', hue='Team', data=top_ast_tov_players, palette='plasma', dodge=True)
                plt.title('Top 10 Players by Average Assist-to-Turnover Ratio')
                plt.ylabel('Average AST/TOV Ratio')
                plt.xlabel('Player Number')
                plt.xticks(rotation=45, ha='right')
                plt.legend(title="Team", bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
                plt.savefig("Top_Players_AST_TOV_Ratio.png")
                plt.close() # Close the figure
                print("✅ Top 10 Players by Average Assist-to-Turnover Ratio plot saved as Top_Players_AST_TOV_Ratio.png")
            else:
                print("No valid data for Top 10 Players by Average Assist-to-Turnover Ratio plot (filtered out 0s/NaNs).")

            # --- Bar Chart: Top 10 Players by Average Rebounding Percentage ---
            print("\nGenerating Top 10 Players by Average Rebounding Percentage plot...")
            top_reb_pct_players = player_advanced_stats_avg.sort_values(by='Avg_REB_Percentage', ascending=False).head(10)
            
            if not top_reb_pct_players.empty:
                plt.figure(figsize=(12, 7))
                sns.barplot(x='Player No.', y='Avg_REB_Percentage', hue='Team', data=top_reb_pct_players, palette='magma', dodge=True)
                plt.title('Top 10 Players by Average Rebounding Percentage')
                plt.xlabel('Player Number')
                plt.ylabel('Average Rebounding Percentage')
                plt.xticks(rotation=45, ha='right')
                plt.legend(title="Team", bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
                plt.savefig("Top_Players_Rebounding_Percentage.png")
                plt.close() # Close the figure
                print("✅ Top 10 Players by Average Rebounding Percentage plot saved as Top_Players_Rebounding_Percentage.png")
            else:
                print("No valid data for Top 10 Players by Average Rebounding Percentage plot.")

            # --- Bar Chart: Top 10 Players by Average Simplified Win Shares (WS_Simplified) ---
            print("\nGenerating Top 10 Players by Average Simplified Win Shares plot...")
            top_ws_players = player_advanced_stats_avg.sort_values(by='Avg_WS_Simplified', ascending=False).head(10)
            if not top_ws_players.empty:
                plt.figure(figsize=(12, 7))
                sns.barplot(x='Player No.', y='Avg_WS_Simplified', hue='Team', data=top_ws_players, palette='rocket', dodge=True)
                plt.title('Top 10 Players by Average Simplified Win Shares')
                plt.ylabel('Average Simplified Win Shares')
                plt.xlabel('Player Number')
                plt.xticks(rotation=45, ha='right')
                plt.legend(title="Team", bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
                plt.savefig("Top_Players_WS_Simplified.png")
                plt.close() # Close the figure
                print("✅ Top 10 Players by Average Simplified Win Shares plot saved as Top_Players_WS_Simplified.png")
            else:
                print("No valid data for Top 10 Players by Average Simplified Win Shares plot.")

            # --- Bar Chart: Top 10 Players by Average Simplified VORP (VORP_Simplified) ---
            print("\nGenerating Top 10 Players by Average Simplified VORP plot...")
            top_vorp_players = player_advanced_stats_avg.sort_values(by='Avg_VORP_Simplified', ascending=False).head(10)
            if not top_vorp_players.empty:
                plt.figure(figsize=(12, 7))
                sns.barplot(x='Player No.', y='Avg_VORP_Simplified', hue='Team', data=top_vorp_players, palette='mako', dodge=True)
                plt.title('Top Players by Average Simplified VORP')
                plt.ylabel('Average Simplified VORP')
                plt.xlabel('Player Number')
                plt.xticks(rotation=45, ha='right')
                plt.legend(title="Team", bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
                plt.savefig("Top_Players_VORP_Simplified.png")
                plt.close() # Close the figure
                print("✅ Top 10 Players by Average Simplified VORP plot saved as Top_Players_VORP_Simplified.png")
            else:
                print("No valid data for Top 10 Players by Average Simplified VORP plot.")

print("\n--- New Visualizations Generation Complete ---")


# --- Main Execution Flow ---
if __name__ == "__main__":
    load_and_explore_data()
    extract_team_names() # This populates xls and df_team_rows
    
    process_box_scores() # This populates df_combined with basic stats and Efficiency
    validate_and_finalize_data() # This further refines df_combined
    calculate_advanced_stats() # This updates df_combined with advanced stats (including WS and VORP)
    prepare_visual_data() # This populates df_visual from df_combined (now with all stats and Player_Team_Label)
    generate_summary_statistics() # Uses df_combined
    generate_advanced_summary_statistics() # Uses df_combined to create player_advanced_stats_avg and saves it
    generate_advanced_visualizations() # Uses df_visual
    generate_final_visualizations() # Uses df_visual (and implicitly player_advanced_stats_avg if it exists)
    generate_new_visualizations() # Uses player_advanced_stats_avg
    
    print("\n--- GameChanger ETL & EDA Script Finished ---")
