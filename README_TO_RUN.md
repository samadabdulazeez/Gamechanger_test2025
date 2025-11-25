# 🏀 Basketball Analytics Dashboard - Easy Setup Guide

Hey! Here's how to run the basketball dashboard on your computer:

## Quick Start (3 Steps)

### Step 1: Install Python
1. Go to [python.org](https://www.python.org/downloads/)
2. Click the big yellow "Download Python" button
3. Run the installer you downloaded
4. **IMPORTANT**: Check the box that says "Add Python to PATH" during installation
5. Click "Install Now"

### Step 2: Download the Files
1. Put all the files I sent you in a folder on your computer
2. Open Command Prompt (search for "cmd" in Windows search)
3. Navigate to your folder by typing:
   ```
   cd "C:\path\to\your\folder"
   ```
   (Replace with the actual path to your folder)

### Step 3: Install and Run
Copy and paste these commands one by one:

```
pip install pandas numpy matplotlib seaborn requests streamlit openpyxl
```

Then:

```
python run_dashboard.py
```

## What Happens Next

1. The program will download basketball data and process it (this takes a minute)
2. Your web browser will automatically open with the dashboard
3. You can explore player stats, team comparisons, and cool charts!

## If Something Goes Wrong

**"python is not recognized"**
- Try using `py` instead of `python`
- Or reinstall Python and make sure to check "Add to PATH"

**"pip is not recognized"**
- Try: `py -m pip install pandas numpy matplotlib seaborn requests streamlit openpyxl`

**Dashboard doesn't open in browser**
- Look for a URL in the command prompt (like `http://localhost:8501`)
- Copy and paste that URL into your browser

## What You'll See

The dashboard lets you:
- Pick your favorite team and player
- Compare them to opponents
- See shooting percentages, rebounds, assists, and more
- View cool charts and graphs

Just close the command prompt window when you're done!

---

**Need help?** Just send me a screenshot of any error messages you see! 😊 