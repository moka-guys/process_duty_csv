"""
Process duty emails
Developers: Bioinformaics Team - Guy's Hospital
Email: igor.malashchuk@nhs.net
Date Modified: 17/11/2022
"""
import subprocess
import os
from os import path
import re
import collections
from datetime import datetime
import pandas
import math
import numpy as np
import config

# Run script in Powershell: duty
# run script in command prompt: S:\Genetics_Data2\Array\Software\Python-3.6.5\python S:\Genetics_Data2\Array\Software\duty_bioinformatician_scripts\process_duty_email.py
version = "1.0.1"

separator = "£$%"

def ask_for_folder():
    """
    For MokaPipe the destination folder can be different.
    Therefore this function ask the user to enter the last part of the folder
    """
    output_folder = ""
    path_worksheets = config.MokaPipe_path_worksheets
    print("Part of destination is:{}".format(path_worksheets))
    while len(output_folder) == 0:
        output_folder = input(
            'Please complete the final part of the destination folder \n(e.g."NGS_401 to 500\\NGS484")\nMay need to place inside double quotes in Powershell:'
        )
        destination_folder = os.path.join(path_worksheets, output_folder)
        print("The destination folder is : {}".format(destination_folder))
        if destination_folder == path_worksheets or not os.path.isdir(destination_folder):
            print("The directory does not exist. Please try again!")
            output_folder = ""
    return os.path.join(path_worksheets, output_folder)

def get_data(df, path_to_folder, site_filter='none'):
    """
    This function reads the data frame (df)..
    From the df file it obtains the download links and creates a string argument.
    site_filter determines if the urls should be filtered based on the
    destination e.g. StG transfer   
    """
    
    urls = ""
    for index, row in df.iterrows():
        if site_filter=='none':
            line = "{},{}{}".format(row['url'], path_to_folder, separator)
            urls += line
        elif site_filter=='StG':
            if bool(re.search("StG", row["url"])):  
                line = "{},{}{}".format(row['url'], path_to_folder, separator)
                urls += line
        elif site_filter=='GSTT':
            if not bool(re.search("StG", row["url"])):  
                line = "{},{}{}".format(row['url'], path_to_folder, separator)
                urls += line
        else:
            print("Pattern not recognised for 'site_filter' in function 'get_data'")
            break
    return urls

def download_data(all_urls):
    """"
    This function checks if the string argument is less than the limit. 
    If it is longer than the character length limit thee string is split into multiple smaller string arguments. 
    The function then downloads the data using powershell.
    """
    limit = 8000
    result = math.ceil(len(all_urls)/limit)
    if result <= 1:
        urls_array = all_urls.split(separator)
        merged_array = " ".join(urls_array)
        urls_list = [merged_array]
    else:
        urls_array = all_urls.split(separator)
        np_array = np.array(urls_array)
        split_np_array = np.array_split(np_array, result)
        urls_list = []
        for one_array in split_np_array:
            merged_sub_array = " ".join(one_array)
            urls_list.append(merged_sub_array)
    full_process = "Version {}\n".format(version)
    for urls in urls_list:
        try:
            process = subprocess.check_output(
                    [
                        "powershell",
                        #"getDNAnexusURL",
                        "S:\\Genetics_Data2\\Array\\Software\\duty_bioinformatician_scripts\\get_DNAnexus_url.ps1",
                        urls,
                    ], 
                    shell=True,
                    stderr=subprocess.STDOUT
                ) 
            full_process += str(process.decode('utf-8') + "\n")
        except subprocess.CalledProcessError as e:
            raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
    return full_process



def save_log_file(text, filename): 
    cur_time = datetime.now()
    cur_time_string = cur_time.strftime("%Y_%m_%d__%H_%M_%S_")
    log_path = "{}\\process_logs\\Finished_on_{}_{}.txt".format(config.CSVread_folder, cur_time_string, filename)
    with open(log_path, 'w') as f:
        f.write(text)

def get_files():
    """
    This function gets the paths for csv files download by duty bioinformatician to the Duty_Bioinformatics_CSV folder.
    """
    folder = config.CSVread_folder
    files = (
        file
        for file in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, file))
    )
    files_list = collections.defaultdict(list)
    for filename in files:
        results = re.search("__(\S+)__", filename) # looks for project type (e.g. WES, SNP, TSO500, MokaPie) in the CSV file name
        project = results.group(1)
        filepath = os.path.join(folder, filename)
        files_list[project].append(filepath)
    files_merged = dict(files_list)
    return files_merged

class Project:
    """
    Process project
    """
    def __init__(self, project, files):
        self.project = project
        self.project_files = files
    def process_data(self):
        """
        This function executes the processing of a project based on the project type e.g. SNP, WES, TSO500 or MokaPipe
        """
        if self.project == "WES":
            self.process_WES()
        elif self.project == "SNP":
            self.process_SNP()
        elif self.project == "TSO500":
            self.process_TSO()
        elif self.project == "MokaPipe":
            self.process_MokaPipe()
    def process_WES(self):
        """
        Process WES Runs
        """
        for file_path in self.project_files:
            # reads the csv file into df
            csv_file = pandas.read_csv(file_path, index_col=None) 
            # extracts urls from the df and creates a string with the download url and \
            #  destination path for the downloaded file:
            url_wes = get_data(csv_file, '"{}"'.format(config.WES_destination_of_files)) 
            # download the file using the url into the destination folder and \
            # capture output from Powershell using Subprocess to be saved to a logfile:
            output = download_data(url_wes)
            # splits the filepath to obtain the filename
            list_of_stings = file_path.split("\\")
            save_log_file(output, list_of_stings[-1].replace('.csv','.txt'))
            print(output)
    def process_SNP(self):
        """
        Process SNP Runs
        for detailed comments see process_WES
        """
        for file_path in self.project_files:
            csv_file = pandas.read_csv(file_path, index_col=None)
            url_snp = get_data(csv_file, '"{}"'.format(config.SNP_destination_of_VCFs))
            output = download_data(url_snp)
            list_of_stings = file_path.split("\\")
            save_log_file(output, list_of_stings[-1].replace('.csv','.txt'))
            print(output)
    def process_MokaPipe(self):
        """
        Process MokaPipe Runs
        for detailed comments see process_WES
        """
        # Path to St George's transfer folder
        StG_transfer = config.StG_transfer
        for file_path in self.project_files:
            # asks for final part to the destination folder
            output_folder = ask_for_folder()
             # create directory for RPKM and download data
            if bool(re.search("NGS\w+_NGS", file_path)): 
                print("WARNING! More than one NGS project has been found in the file name.\n \
                    This script currently does not support this operation.\n \
                    Possible solution: manually split the csv file into separate\n \
                    projects and rename the files to contain onty one NGS project.\n \
                    e.g. 002_211119_A01229_0050_BHMYNGDRXY_NGS446_NGS445A_NGS444.csv\n \
                    to become: 002_211119_A01229_0050_BHMYNGDRXY_NGS446.csv \n \
                               002_211119_A01229_0050_BHMYNGDRXY_NGS445A.csv \n \
                               002_211119_A01229_0050_BHMYNGDRXY_NGS444.csv")
                break
            else:
                path_to_RPKM = os.path.join(output_folder, "RPKM")
                destination_of_Coverage = os.path.join(output_folder, "coverage")
                if not path.exists(path_to_RPKM):
                    os.mkdir(path_to_RPKM)
                if not path.exists(destination_of_Coverage):
                    os.mkdir(destination_of_Coverage)
                # import csv file to pandas df
                csv_file = pandas.read_csv(file_path, index_col=None)
                # get data frames from the master df
                df_RPKM = csv_file[csv_file['type'] == 'RPKM']
                df_coverage = csv_file[csv_file['type'] == 'coverage']
                df_FHPRS = csv_file[csv_file['type'] == 'FH_PRS']
                # Create a folder with NGS run name, subfolders cold coverasge and RPKM, download data
                results = re.search(r"\\(NGS\S+)$", output_folder)
                foldername = results.group(1)
                # Find if any files are required for StG trasnfer
                StGsearch = csv_file['url'].str.contains('StG').any()
                if StGsearch:
                    StG_transfer_folder = os.path.join(StG_transfer, foldername)
                    # Make directories in the outgoing folder
                    if not path.exists(StG_transfer_folder):
                        os.mkdir(StG_transfer_folder)
                    if not path.exists(os.path.join(StG_transfer_folder, "coverage")):
                        os.mkdir(os.path.join(StG_transfer_folder, "coverage"))
                    if not path.exists(os.path.join(StG_transfer_folder, "RPKM")):
                        os.mkdir(os.path.join(StG_transfer_folder, "RPKM"))
                    url_StG_RPKM = get_data(df_RPKM, '"{}"'.format(os.path.join(StG_transfer_folder, "RPKM")))
                    url_StG_coverage = get_data(df_coverage, '"{}"'.format(os.path.join(StG_transfer_folder, "coverage")), 'StG')
                else:
                    url_StG_RPKM = ''
                    url_StG_coverage = ''
                # Download data for StG Transfer
                url_RPKM = get_data(df_RPKM, '"{}"'.format(path_to_RPKM))
                url_coverage = get_data(df_coverage, '"{}"'.format(destination_of_Coverage), "GSTT")
                if len(df_FHPRS.index) >= 1 :
                    destination_of_FHPRS = os.path.join(output_folder, "FH_PRS")
                    if not path.exists(destination_of_FHPRS):
                        os.mkdir(destination_of_FHPRS)
                    if StGsearch:
                        if not path.exists(os.path.join(StG_transfer_folder, "FH_PRS")):
                            os.mkdir(os.path.join(StG_transfer_folder, "FH_PRS"))
                        url_StG_FHPRS = get_data(df_FHPRS, '"{}"'.format(os.path.join(StG_transfer_folder, "FH_PRS")), 'StG')
                    else:
                        url_StG_FHPRS = ''
                    url_FHPRS = get_data(df_FHPRS, '"{}"'.format(destination_of_FHPRS), 'GSTT') 
                else:
                    url_FHPRS = ''
                    url_StG_FHPRS = ''
                all_url =  url_RPKM + url_coverage + url_FHPRS + url_StG_RPKM + url_StG_coverage + url_StG_FHPRS
                output = download_data(all_url)
                list_of_stings = file_path.split("\\")
                save_log_file(output, list_of_stings[-1].replace('.csv','.txt'))
                print(output)
    def process_TSO(self):
        """
        Process TSO500 Runs
        for detailed comments see process_WES
        """
        # destination for the files to be downloaded to:
        destination_of_Results = config.TSO_destination_of_Results
        # create folder woth project name
        for file_path in self.project_files:
            csv_file = pandas.read_csv(file_path, index_col=None)
            df_results = csv_file[csv_file['type'] == 'Results']
            df_coverage = csv_file[csv_file['type'] == 'coverage']
            df_sompy = csv_file[csv_file['type'] == 'sompy']
            search_results1 = re.search(r"_(TSO\S+).csv", file_path)
            search_results2 = re.search(r"002(_\S+)_TSO", file_path)
            folder_name = "{}{}".format(
            search_results1.group(1),search_results2.group(1)
            )
            path_to_folder = os.path.join(destination_of_Results, folder_name)
            # create subfolders called coverage and Results and download data:
            destination_of_Coverage = os.path.join(path_to_folder, "coverage")
            destination_of_extracted_Results = os.path.join(path_to_folder, "Results")
            destination_of_sompy = path_to_folder
            os.mkdir(path_to_folder)
            os.mkdir(destination_of_Coverage)
            os.mkdir(destination_of_extracted_Results)
            url_coverage = get_data(df_coverage, '"{}"'.format(destination_of_Coverage))
            url_sompy = get_data(df_sompy, '"{}"'.format(destination_of_sompy))
            url_results = get_data(df_results, '"{}"'.format(path_to_folder))
            all_url = url_coverage + url_sompy + url_results
            output = download_data(all_url)
            list_of_stings = file_path.split("\\")
            save_log_file(output, list_of_stings[-1].replace('.csv','.txt'))
            print(output)
    def archive_files(self):
        """
        Modify the csv file to remove data from it and insert a message of when it was compelted.
        """
        # get the user infromation and current date
        #user = os.getlogin() -> does not work when executing the script from PowerShell
        cur_time = datetime.now()
        cur_time_string = cur_time.strftime("%Y_%m_%d__%H_%M_%S_")
        # message to be added to the csv file
        message = "Processed_on_{}".format(
            cur_time_string
        )
        # Repalce data in csv file with the message and move the files to archive folder.
        for file_path in self.project_files:
            #subprocess.Popen("echo {} > {}".format(message, file_path))
            file_path_list = file_path.split("\\")
            new_name = message+file_path_list[3]
            print(new_name)
            file_path_list = file_path_list[:3] + ["archive"] + [new_name]
            archive_file_path = "\\".join(file_path_list)
            os.rename(file_path, archive_file_path)
        
if __name__ == "__main__":

    data = get_files()
    print(data)
    for key in data:
        project = Project(key, data[key])
        project.process_data()
        project.archive_files()
