""" process_duty_email.py

Process duty emails
Developers: Bioinformaics Team - Guy's Hospital
"""
import subprocess
import os
import datetime
import argparse
from ast import literal_eval
import pandas as pd
import easygui
import config
from logger import Logger


class ProcessCSV:
    """"""

    def __init__(self):
        """"""
        self.csv_name = easygui.enterbox(
            "What is the name of the CSV file you would like to process?"
        )
        self.csv_path = f"{config.CSV_FOLDER}{self.csv_name}"
        self.archive_csv_path = f"{config.ARCHIVE_FOLDER}{self.csv_name}"
        self.timestamp = datetime.datetime.now().strftime("%d-%B-%Y %H:%M:%S")
        self.logfile_path = (
            f"{self.csv_path.split('.duty_')[0]}."
            f"process_duty_{self.timestamp}.log"
        )
        self.archive_logfile_path = (
            f"{config.ARCHIVE_FOLDER}%s",
            self.logfile_path.split("\\")[-1],
        )
        self.logger = Logger(self.logfile_path).logger
        self.logger.info("Running duty_csv %s", git_tag())
        # Read in dataframe with GSTT_dir column as dtype list, then explode
        # so that each list item (directory destination) has its own row
        self.dataframe = pd.read_csv(
            self.csv_path, converters={"GSTT_dir": literal_eval}
        ).explode("GSTT_dir")
        self.run_name = self.get_runfolder_dir()
        self.command_list = self.create_commands()
        self.failure_list = self.download_data()
        # TODO may need to add a command to create the output dir
        self.archive_files()

    def get_runfolder_dir(self):
        """
        Process WES Runs
        """
        # Check if string conversion is in filepaths
        if self.dataframe["GSTT_dir"].str.contains("%s").any():
            run_name = easygui.enterbox("What is the name of the run?")
        else:
            run_name = False
        return run_name

    def create_commands(self):
        """
        Create download commands and add to dataframe as 'command' column
        """
        # Generate download commands
        command_list = (
            "Start-BitsTransfer -Source "
            + self.dataframe["Url"]
            + " -Destination "
            + self.dataframe["GSTT_dir"] % self.run_name
        ).tolist()

        # Generate unzip commands
        # Find CSV rows with zip files
        zip_rows = self.dataframe[self.dataframe["Url"].str.contains("zip")]
        # Generate unzip commands
        unzip_cmds = (
            "Expand-Archive -LiteralPath "
            + self.dataframe["GSTT_dir"] % self.run_name
            + zip_rows["Url"].split("/")[-1]
            + " -DestinationPath "
            + self.dataframe["GSTT_dir"] % self.run_name
        ).tolist()

        command_list.extend(unzip_cmds)

        return command_list

    def download_data(self):
        """
        Downloads data by running each command in the dataframe using
        subprocesses
        """
        failure_list = []
        for command in self.command_list:
            if not self.run_subprocess(command):
                failure_list.append(command)
        return failure_list

    def run_subprocess(self, command):
        """
        Execute command using subprocess.Popen() and communicate output

            :param command (str):               Command string
            :return (stdout,stderr) (tuple):    Standard out and standard
        """
        try:
            out, err = subprocess.Popen(
                [command],
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                shell=True,
                executable="powershell.exe",
            ).communicate()
            self.logger.info(out)
            self.logger.info(err)
            return True
        except subprocess.CalledProcessError as error:
            self.logger.error(
                "Command %s returned error (code %s): %s",
                error.cmd,
                error.returncode,
                error.output,
            )

    def archive_files(self):
        """
        Move the CSV file and logfile to the archive folder
        """
        if self.failure_list:
            self.logger.error(
                "The following commands did not execute successfully: %s",
                self.failure_list,
            )
        else:
            self.logger.info("All files downloaded successfully")
            os.rename(self.csv_path, self.archive_csv_path)
            os.rename(self.logfile_path, self.archive_logfile_path)


def git_tag():
    """Obtain git tag from current commit
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


if __name__ == "__main__":
    ProcessCSV()
