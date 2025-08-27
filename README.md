# 1) cd into the folder where you saved the script
cd /path/to/your/project

# 2) (optional but recommended) create & activate a venv
python3 -m venv venv
source venv/bin/activate

# 3) install deps
pip install requests python-dotenv

# 4) create .env in the SAME folder as the script
#    (use the template below)

# 5) run the script
python3 alerts.py


How to use it"

Start fresh and alert if already beyond ±30% on first run:

python3 alerts.py --threshold 0.30 --window 5 --interval 300 --startup over --reset-state


If you want first-run alerts for everything regardless of value:

python3 alerts.py --startup all --reset-state (preferred)


If you never want first-run alerts unless there’s a true crossing:

python3 alerts.py --startup none --reset-state