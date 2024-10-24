from Expand_Database import ExpandDatabase
from Process_Data import ProcessData
from Generate_Heatmaps import GenerateHeatmaps

def main():
    # Call the main functions of the other scripts
    ExpandDatabase.expand_database()
    ProcessData.process_data()
    GenerateHeatmaps.generate_heatmaps()
    GenerateHeatmaps.generate_html()

if __name__ == "__main__":
    main()
