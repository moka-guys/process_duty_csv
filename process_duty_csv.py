""" process_duty_csv.py

Processes the end of duty CSV files generated by duty_csv, downloading them
to locations defined in the CSV
"""
import sys
import subprocess
import os
import datetime
import tkinter as tk
from tkinter import filedialog
import pandas as pd
import config
from logger import Logger


class ProcessCSV:
    """
    Class for processing the CSV generated by the duty_csv dnanexus app

    Methods
        get_csv_path()
            Get CSV path from user input using filedialog
        get_scriptmode()
            Read the CSV file to determine presence or absence of "testing"
            substring. If present, return "TEST" mode, else "PROD" mode.
        get_dataframe()
            Read in dataframe from CSV
        complete_gstt_paths()
            If string concatenation string (%s) is in the directory path in the
            dataframe, collect subdirecctory name (/s) from the user using
            tkinter input boxes. Then input these into the directory paths
            within the dataframe
        collect_tkinter_var()
            Call the GetTkinterEntry function to return the variable input
            into the message box by the user
        create_command_list()
            Create powershell commands
        create_download_commands()
            For each file in the dataframe, check GSTT directory path is valid,
            if so add the subdir to the path, and create the powershell
            download command for that file. Append to download commands list
        valid_path()
            Validation of path using os
        create_unzip_commands()
            Generate unzip commands
        create_dirs()
            Create subdirectories specified in the dataframe if they
            don't already exist
        write_cmds_to_file()
            Write commands from self.command_list to file for audit trail
        download_data()
            Set off each command in self.command_list as a child process
        run_process()
            Execute command as child process using os.system()
            and communicate output
        archive_csv()
            Move the CSV file and logfile to the archive folder
    """

    def __init__(self):
        """"""
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.csv_path = self.get_csv_path()
        self.csv_name = self.csv_path.rsplit("/", 1)[1]
        self.script_mode = self.get_scriptmode()
        self.logfile_path = (
            f"{config.DIRS['LOGS'] % config.CSV_FOLDER[self.script_mode]}"
            f"{self.csv_name.split('.duty_')[0]}."
            f"process_duty_{self.timestamp}.log"
        )
        self.logger_obj = Logger(self.logfile_path)
        self.logger = self.logger_obj.logger
        self.logger.info("Running process_duty_csv %s", git_tag())
        self.logger.info("The logfile path is %s", self.logfile_path)
        self.archive_csv_path = (
            f"{config.DIRS['ARCHIVE'] % config.CSV_FOLDER[self.script_mode]}"
            f"{self.csv_name}"
        )
        self.logger.info("The CSV archive path is %s", self.archive_csv_path)
        self.dataframe = self.get_dataframe()
        self.cmds_filepath = (
            f"{config.DIRS['CMDS'] % config.CSV_FOLDER[self.script_mode]}"
            f"{self.csv_name.split('.duty_')[1]}."
            f"process_duty_csv_cmds_{self.timestamp}.ps1"
        )
        self.complete_gstt_paths()
        self.command_list = self.create_command_list()
        self.create_dirs()
        self.write_cmds_to_file()
        self.download_data()
        # Files will only archive if other methods have completed successfully
        self.archive_csv()
        self.logger.info("Script has completed successfully")

    def get_csv_path(self) -> str:
        """
        Get CSV path from user input using filedialog
            :return csv_path(str): Path to CSV file
        """
        csv_path = filedialog.askopenfilename(
            initialdir=config.CSV_FOLDER["PROD"],
            title="Select file",
            filetypes=(("CSV files", "*.csv"), ("all files", "*.*")),
        )
        if self.csv_path:
            self.logger.info("The CSV path is %s", csv_path)
            return csv_path
        else:
            self.logger.error("The user did not provide a CSV path")
            sys.exit(1)

    def get_scriptmode(self) -> str:
        """
        Read the CSV file to determine presence or absence of "testing"
        substring. If present, return "TEST" mode, else "PROD" mode.
            :return mode(str):  Script mode
        """
        try:
            with open(self.csv_path, "r", encoding="utf-8") as file:
                if "testing" in file.read():
                    mode = "TEST"
                else:
                    mode = "PROD"
                self.logger.info("The script is being run in %s mode", mode)
                return mode
        except Exception as exception:
            self.logger.error(
                "%s was raised when loading the CSV as a "
                "pandas dataframe: %s",
                type(exception).__name__,
                exception,
            )
            sys.exit(1)

    def get_dataframe(self) -> pd.core.frame.DataFrame:
        """
        Read in dataframe from CSV
            :return (pd.core.frame.DataFrame): Dataframe
        """
        try:
            dataframe = pd.read_csv(self.csv_path)
            return dataframe
        except Exception as exception:
            self.logger.error(
                "%s was raised when loading the CSV as a "
                "pandas dataframe: %s",
                type(exception).__name__,
                exception,
            )
            sys.exit(1)

    def complete_gstt_paths(self) -> None:
        """
        If string concatenation string (%s) is in the directory path in the
        dataframe, collect subdirecctory name (/s) from the user using tkinter
        input boxes. Then input these into the directory paths within the
        dataframe
            :return None:
        """
        try:
            # Requires multiple subdirectory inputs (NGS worksheets runfolder
            # range, and runfolder name)
            if self.dataframe["GSTT_dir"].str.contains(r"/%s%s/").any():
                self.logger.info("Getting worksheets subdir")
                worksheets_dir = self.collect_tkinter_var(
                    config.WORKSHEETS_DIR_LABEL
                )
                self.logger.info("Getting runfolder subdir")
                runfolder_dir = self.collect_tkinter_var(
                    config.RUNFOLDER_DIR_LABEL
                )
                # Insert directories into dataframe filepath strings
                self.dataframe["GSTT_dir"] = self.dataframe[
                    "GSTT_dir"
                ].str.replace(r"%s%s", rf"{worksheets_dir}/{runfolder_dir}")
            # Only requires a single subdirectory input (runfolder name)
            if self.dataframe["GSTT_dir"].str.contains(r"/%s/").any():
                # If runfolder_dir has already been collected don't open
                # another message box
                if "runfolder_dir" in locals():
                    self.dataframe["GSTT_dir"] = self.dataframe[
                        "GSTT_dir"
                    ].str.replace(r"%s", runfolder_dir)
                else:
                    self.logger.info("Getting runfolder subdir")
                    runfolder_dir = self.collect_tkinter_var(
                        config.RUNFOLDER_DIR_LABEL
                    )
                    # Insert runfolder_dir into dataframe
                    self.dataframe["GSTT_dir"] = self.dataframe[
                        "GSTT_dir"
                    ].str.replace(r"%s", runfolder_dir)
        except Exception as exception:
            self.logger.error(
                "%s was raised when completing "
                "the GSTT paths using user inputs: %s",
                type(exception).__name__,
                exception,
            )
            sys.exit(1)

    def collect_tkinter_var(self, label) -> str:
        """
        Call the GetTkinterEntry function to return the variable input
        into the message box by the user
            :return (str): Contents from Tkinter Entry
        """
        try:
            master = tk.Tk()
            entry = GetTkinterEntry(master, label, self.logger)
            master.protocol("WM_DELETE_WINDOW", entry.on_close)
            master.mainloop()
            self.logger.info(
                "The following user input was collected: %s",
                entry.entry_contents,
            )
            return entry.entry_contents
        except Exception as exception:
            self.logger.error(
                "%s was raised when collecting the user input: %s",
                type(exception).__name__,
                exception,
            )
            sys.exit(1)

    def create_command_list(self) -> list:
        """
        Create powershell commands
            :return command_list(list): Powershell commands
        """
        try:
            command_list = self.create_download_commands().extend(
                self.create_unzip_commands()
            )
            return command_list
        except Exception as exception:
            self.logger.error(
                "%s was raised when extending the "
                "powershell commands list: %s",
                type(exception).__name__,
                exception,
            )
            sys.exit(1)

    def create_download_commands(self) -> list:
        """
        For each file in the dataframe, check GSTT directory path is valid, if
        so add the subdir to the path, and create the powershell download
        command for that file. Append to the download commands list
            :return download_cmds(list): Powershell download commands
        """
        try:
            download_cmds = []
            for path in self.dataframe["GSTT_dir"]:
                self.valid_path(path)
                self.dataframe["GSTT_dir"] = (
                    self.dataframe["GSTT_dir"] + self.dataframe["subdir"]
                )
                # Generate download cmds
                download_cmds.append(
                    "powershell Start-BitsTransfer -Source '"
                    + self.dataframe["Url"]
                    + "' -Destination '"
                    + self.dataframe["GSTT_dir"]
                    + "'"
                )
            return download_cmds
        except Exception as exception:
            self.logger.error(
                "%s was raised when creating the powershell "
                "DNAnexus file download commands: %s",
                type(exception).__name__,
                exception,
            )
            sys.exit(1)

    def valid_path(self, path) -> True:
        """
        Validation of path using os
            :return True:
        """
        if os.path.isabs(path) and os.path.exists(path):
            return True
        else:
            self.logger.error("Path does not exist on this system: %s", path)
            sys.exit(1)

    def create_unzip_commands(self) -> list:
        """
        Generate unzip commands
            :return unzip_cmds(list): Powershell unzip commands
        """
        try:
            unzip_cmds = []
            # Drop rows that aren't a zip file
            zip_files = self.dataframe[
                self.dataframe["Url"].str.contains(".zip")
            ]
            if not zip_files.empty:
                unzip_cmds.append(
                    "Expand-Archive -LiteralPath "
                    + self.dataframe["GSTT_dir"]
                    + zip_files
                    + " -DestinationPath "
                    + self.dataframe["GSTT_dir"]
                ).tolist()
            return unzip_cmds
        except Exception as exception:
            self.logger.error(
                "%s was raised when creating the powershell "
                "file unzip commands commands: %s",
                type(exception).__name__,
                exception,
            )
            sys.exit(1)

    def create_dirs(self) -> None:
        """
        Create subdirectories specified in the dataframe if they
        don't already exist
            :return None:
        """
        for directory in self.dataframe["GSTT_dir"]:
            if not os.path.exists(directory):
                try:
                    os.mkdir(directory)
                    self.logger.info(
                        "The following directory was created: %s", directory
                    )
                except Exception as exception:
                    self.logger.error(
                        "%s was raised when trying to create "
                        "the directory %s: %s",
                        type(exception).__name__,
                        directory,
                        exception,
                    )
                    sys.exit(1)

    def write_cmds_to_file(self) -> None:
        """
        Write commands from self.command_list to file for audit trail
            :return None:
        """
        try:
            with open(self.cmds_filepath, "w+", encoding="utf-8") as file:
                for item in self.command_list:
                    file.write(f"{item}\n")
        except Exception as exception:
            self.logger.error(
                "%s was raised when writing powershell commands "
                "to file (%s): %s",
                type(exception).__name__,
                self.cmds_filepath,
                exception,
            )
            sys.exit(1)

    def download_data(self) -> None:
        """
        Set off each command in self.command_list as a child process
            :return None:
        """
        for command in self.command_list:
            self.run_process(command)
        self.logger.info("All commands executed without error")

    def run_process(self, command: str) -> None:
        """
        Execute command as child process using os.system()
        and communicate output
            :return None:
        """
        self.logger.info("Running the following command: %s", command)
        attempts = 1
        while attempts < 6:
            try:
                # Shutdown logger to allow appending to file
                self.logger_obj.shutdown_logs()
                returncode = os.system(f"{command} >> {self.logfile_path}")
                self.logger_obj = Logger(self.logfile_path)
                self.logger = self.logger_obj.logger
                if returncode == 0:
                    self.logger.info(
                        "Command executed without error. Returncode: %s",
                        returncode,
                    )
                    break
                else:
                    self.logger.error(
                        "An error was encountered, with returncode %s",
                        returncode,
                    )
                    if attempts < 5:
                        self.logger.info(
                            "Trying again. Attempt %s", attempts + 1
                        )
                    else:
                        sys.exit(1)
                    attempts += 1
            except Exception as exception:
                self.logger.error(
                    "%s was raised when running the command: %s",
                    type(exception).__name__,
                    exception,
                )

    def archive_csv(self) -> None:
        """
        Move the CSV file and logfile to the archive folder
            :return None:
        """
        try:
            os.rename(self.csv_path, self.archive_csv_path)
            self.logger.info(
                "Success - %s moved to %s",
                self.csv_path,
                self.archive_csv_path,
            )
        except Exception as exception:
            self.logger.error(
                "%s was raised when archiving %s to %s: %s",
                type(exception).__name__,
                self.csv_path,
                self.archive_csv_path,
                exception,
            )
            sys.exit(1)


def git_tag() -> str:
    """
    Obtain git tag from current commit
        :return stdout (str):   String containing stdout,
                                with newline characters removed
    """
    filepath = os.path.dirname(os.path.realpath(__file__))
    cmd = f"git -C {filepath} describe --tags"

    proc = subprocess.Popen(
        [cmd], stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True
    )
    out, _ = proc.communicate()
    return out.rstrip().decode("utf-8")


class GetTkinterEntry:
    """
    Class to collect the user input from a Tkinter entry as a variable

    Methods
        callback()
            Get the contents of the Entry and exit
        on_close()
            Open a message box on closing the Tkinter Entry
    """

    def __init__(self, master, label, logger):
        self.master = master
        self.label = label
        self.logger = logger
        self.entry_contents = None
        self.master.title(f"process_duty_csv {git_tag()} input box")
        # Label for input using widget Label
        self.label = tk.Label(
            master, text=self.label, font=("calibre", 10, "bold")
        )
        self.label.grid(row=0, column=0)
        # Entry for input using widget Entry
        self.entry = tk.Entry(master, width=35, font=("calibre", 10, "normal"))
        self.entry.grid(row=0, column=1)

        self.entry.focus_set()

        tk.Button(master, text="Submit", width=10, command=self.callback).grid(
            row=10, column=0
        )

    def callback(self) -> None:
        """
        Get the contents of the Entry and exit
            :return None:
        """
        self.entry_contents = self.entry.get()
        self.master.quit()
        self.master.destroy()

    def on_close(self) -> None:
        """
        Open a message box on closing the Tkinter Entry
            :return None:
        """
        response = tk.messagebox.askyesno(
            "Exit", "Are you sure you want to exit?"
        )
        if response:
            self.master.destroy()
            self.logger.error(
                "The user closed the entry box without providing an input"
            )
            sys.exit(1)


if __name__ == "__main__":
    ProcessCSV()
