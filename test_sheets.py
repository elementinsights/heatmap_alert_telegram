import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import gspread

# Load .env file
load_dotenv()

# Read env vars
creds_path = os.getenv("GOOGLE_SA_JSON")
sheet_id   = os.getenv("GOOGLE_SHEETS_ID")
tab_name   = os.getenv("GOOGLE_SHEETS_TAB", "Alerts")

if not creds_path or not sheet_id:
    raise RuntimeError("Missing GOOGLE_SA_JSON or GOOGLE_SHEETS_ID in .env")

# Authenticate
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
gc = gspread.authorize(creds)

# Open the sheet + worksheet
sh = gc.open_by_key(sheet_id)
try:
    ws = sh.worksheet(tab_name)
except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title=tab_name, rows=2000, cols=20)

# Test writing
ws.append_row(["✅ Test", "Connection", "Successful"])

print("Success! Added a test row to your sheet.")
