# Heatmap Alerts Setup & Usage

## 1. Project Setup

### Navigate to project folder
```bash
cd /path/to/your/project
```

### (Optional but recommended) Create and activate a virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### Install dependencies
```bash
pip install python-dotenv requests gspread google-auth
```

### Create environment file
Create a `.env` file in the **same folder** as the script.  
(Use the provided template.)

---

## 2. Running the Script

### Basic run
```bash
python3 alerts.py
```

### Startup options
- **Alert immediately if already beyond ±30% on first run**:
  ```bash
  python3 alerts.py --threshold 0.30 --window 5 --interval 300 --startup over --reset-state
  ```

- **Alert on all values on first run (preferred)**:
  ```bash
  python3 alerts.py --startup all --reset-state
  ```

- **Only alert on true crossings (ignore first-run values)**:
  ```bash
  python3 alerts.py --startup none --reset-state
  ```

---

## 3. Deployment

### Upload updates (from parent folder on Mac)
```bash
rsync -avz --progress   --exclude 'venv/'   --exclude '__pycache__/'   -e "ssh -i ~/.ssh/heatmap_do"   heatmap1 heatmap2   heatmap@206.189.79.36:/home/heatmap/heatmaps/
```

### Log in to server
```bash
ssh -i ~/.ssh/heatmap_do heatmap@206.189.79.36
```

---

## 4. Service Management (on server)

### Stop scripts
```bash
sudo systemctl stop heatmap1 heatmap2
```

### Start scripts
```bash
sudo systemctl start heatmap1
sudo systemctl start heatmap2
```

### Restart scripts
```bash
sudo systemctl restart heatmap1 heatmap2
```

### Watch logs in real time
```bash
journalctl -u heatmap1 -f
journalctl -u heatmap2 -f
```

### Modify System Files
```bash
sudo nano /etc/systemd/system/heatmap1.service
```

### After Modifying System Files
```bash
sudo systemctl daemon-reload
sudo systemctl enable heatmap1.service
```

### Install venv on Server
```bash
python3 -m venv venv
source venv/bin/activate
```

### Install Dependencies on Server
```bash
pip install --upgrade pip
pip install requests python-dotenv gspread google-auth
```

---

## 5. Running Locally (from `/heatmaps`)

```bash
cd /Users/jasonwuerch/Desktop/Projects/heatmaps/heatmap1
source ../venv/bin/activate
python alerts.py --startup none

cd /Users/jasonwuerch/Desktop/Projects/heatmaps/heatmap2
source ../venv/bin/activate
python alerts.py --startup none
```
