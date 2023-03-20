""" Config file for process_duty_csv
"""
import datetime

LOGGING_FORMATTER = "%(asctime)s - %(levelname)s - %(message)s"
TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

# Folder that the CSV file will be located in
CSV_FOLDER = {
    "TEST": "P:/Bioinformatics/testing/process_duty_csv/",
    "PROD": "P:/Bioinformatics/Duty_Bioinformatics_CSV/",
}

# Logfile subdirs
DIRS = {
    "ARCHIVE": "%sarchive/",
    "CMDS": "%scmds_logs/",
    "LOGS": "%sprocess_logs/",
}

WORKSHEETS_DIR_LABEL = (
    "Please input the NGS worksheets runfolder directory without "
    "quotation marks, e.g.: NGS_501 to 600"
)
RUNFOLDER_DIR_LABEL = (
    "Please input the final part of the destination folder without "
    "quotation marks, e.g.: NGS451 or TSO12345"
)
