from Expand_Database import ExpandDatabase
from Process_Data import ProcessData

def main():
    # Call the main functions of the other scripts
    ExpandDatabase.expand_database()
    ProcessData.process_data()

if __name__ == "__main__":
    main()
