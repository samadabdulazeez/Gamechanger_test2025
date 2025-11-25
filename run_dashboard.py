import subprocess
import sys
import os
import time

def run_gamechanger_and_dashboard():
    """
    Executes gamechanger.py to process data and generate plots for both leagues,
    then launches the Streamlit dashboard app.
    """
    print("--- Starting GameChanger ETL & EDA Process ---")
    print("This may take a moment as data is processed and plots are generated...")
    
    # Define leagues to process
    leagues = ["Rising Stars S1", "Rising Stars S2"]
    
    try:
        # Process each league
        for league in leagues:
            print(f"\n{'='*60}")
            print(f"Processing {league}...")
            print(f"{'='*60}")
            
            # Define the command to run gamechanger.py with league argument
            gamechanger_command = [sys.executable, "gamechanger.py", league]
            
            # Run gamechanger.py for this league
            # capture_output=False means stdout/stderr of gamechanger.py will be streamed to the console
            # check=True means if gamechanger.py returns a non-zero exit code (an error), it will raise an exception
            process_gamechanger = subprocess.run(gamechanger_command, check=True, capture_output=False, text=True)
            print(f"\n✅ {league} processing completed successfully!")
        
        print("\n--- GameChanger ETL & EDA Process Finished Successfully! ---")
        print("Data processed and plots saved for all leagues. Preparing to launch dashboard...")

        # Give a small delay to ensure files are written to disk before Streamlit tries to read them
        time.sleep(2) 

        # Define the command to run the Streamlit dashboard
        # Assuming streamlit is installed and dashboard_app.py is in the same directory
        streamlit_command = [sys.executable, "-m", "streamlit", "run", "dashboard_app.py"]

        print("\n--- Launching Streamlit Dashboard ---")
        print("Please wait for the browser to open or follow the URL provided by Streamlit.")
        
        # Run the Streamlit app. This will typically open a browser window.
        # We don't use check=True here because Streamlit runs indefinitely until stopped manually.
        # We also let it inherit stdout/stderr so the user sees Streamlit's output.
        subprocess.run(streamlit_command)

    except FileNotFoundError:
        print(f"Error: Python interpreter '{sys.executable}' or script not found.")
        print("Please ensure Python is installed and gamechanger.py/dashboard_app.py are in the current directory.")
    except subprocess.CalledProcessError as e:
        print(f"\n--- Error during GameChanger ETL & EDA Process ---")
        print(f"GameChanger script failed with exit code {e.returncode}.")
        print(f"Error output (if any): {e.stderr}")
        print("Please check 'gamechanger.py' for errors and try again.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Check if gamechanger.py and dashboard_app.py exist in the current directory
    if not os.path.exists("gamechanger.py"):
        print("Error: 'gamechanger.py' not found in the current directory. Please ensure it's there.")
        sys.exit(1)
    if not os.path.exists("dashboard_app.py"):
        print("Error: 'dashboard_app.py' not found in the current directory. Please ensure it's there.")
        sys.exit(1)
        
    run_gamechanger_and_dashboard()
    print("\n--- Orchestration script finished. Dashboard should be running or an error occurred. ---")

