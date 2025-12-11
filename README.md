File Utility Tool GUI - Ian Hill, 2025

This program is FREEWARE provided for NON COMMERCIAL use and is provided
WITH NO WARRANTY. This program deletes files. It does
NOT use recycle bin or system trash folders. Use at your own risk.

This program is comprised of a series of utility scripts for file management.

Provided in this git repository are files for the original python script
(fileutilitytoolgui.py) and packaged binary executables for both Windows and
Linux. You may find these executables in the appropriate Windows/Linux
directories under the 'dist' folder.



The utilities are listed as follows:

FILECOMP is a utility for detecting and deleting duplicate files:
    Inputs:
        Starting Directory: Where you want the scan to start
        Database File: The database file you want to use, resume, or create.
            (NOTE: a new database should be generated after each mass delete)
    Options:
        Generate SQL: Scans the specified directory and all subdirectories,
            generating an md5 hash of all files
        Calculate Duplicates: Determine the number of duplicate files based on
            md5 hashes in the database, returns the number of duplicate files
        Delete Duplicates: Delete all duplicate files on the filesystem
            as determined by Calculate Duplicates (requires sufficient
            permissions). Retains the FIRST found copy of a file, deleting all
            other identical (not identically named) files.
    WorkFlow:
        Select/Define starting directory, Set database name/use default,
        Create Database, Calculate Duplicates, Run 'Delete Duplicates'
        if desired. Repeat with new database as needed. If one wishes
        one can scan multiple directories into one database and delete
        duplicate files from all locations as built in one database. 

FILEGROUPER is a utility for copying files to directories based on
        file extension or file name
    Inputs:
        Origin Directory: The location where files to be grouped reside
        Destination Directory: where the files will be moved.
            (NOTE: must not be within the origin directory)
        Database File: Database for storing record of action
    Options:
        Grouping Behavior:
            Dropdown:
             Group by Extension: create directories based on extension
             and copy files to the appropriate file extension
             Group with (dot): adds "dot " to extension file, as an
             original request for this functionality
             Group by filename: create directories by filename, ideally only
             for sorting duplicate names, different files
            Do Not Copy Duplicates or Copy Duplicates:
             Do or Do not make copies of identical files based on their md5   
        Workflow:
            Select/Define source directory, Select Define Destination directory,
            Select/Define database file, chose grouping behavior, run process.
DirectoryCleaner is a utility for deleting empty directories recursively
    (NOTE: Does not use database to record actions or resume)
    Inputs:
        Select directory: The starting directory
    Workflow:
        Select directory and run.


The Output Window displays the operating results of the utilities.
