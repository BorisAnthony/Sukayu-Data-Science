import os
from pathlib import Path

# Absolute path to the utilities directory
UTILITIES_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to scripts directory (one level up from utilities)
SCRIPTS_DIR = os.path.abspath(os.path.join(UTILITIES_DIR, os.pardir))

# Path to project root (one level up from scripts)
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))

# Define all other paths relative to PROJECT_ROOT
DATABASE_DIR = os.path.join(PROJECT_ROOT, "database")
DATABASE_PATH = os.path.join(DATABASE_DIR,           "dist/sukayu_historical_obs_daily_everything.sqlite")

OUTPUTS_DIR = os.path.join(PROJECT_ROOT, "outputs")
FIGURES_DIR = os.path.join(OUTPUTS_DIR,           "figures")
DERIVED_DIR = os.path.join(OUTPUTS_DIR,           "derived")