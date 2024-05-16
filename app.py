import streamlit as st
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark import SparkConf
from pyspark.sql import SparkSession


conf = SparkConf().setAppName("airline").setMaster("local")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Load the PySpark model
dataPipeline = Pipeline.load("modelAndPipeline/dataPipeline")
rfModel = RandomForestRegressionModel.load("modelAndPipeline/rfModel")

origin_place = ['MSY', 'GEG', 'SNA', 'BUR', 'GTF', 'IDA', 'GRR', 'EUG', 'MYR', 'PVD', 'OAK', 'MSN', 'DCA', 'CID', 'HLN', 'RDM', 'ORF', 'CRW', 'SAV', 'CMH', 'PNS', 'LIH', 'IAH', 'HNL', 'CVG', 'SJC', 'LGA', 'BUF', 'RDD', 'AUS', 'MLI', 'SJU', 'LGB', 'GJT', 'RNO', 'SRQ', 'SBN', 'JAC', 'CHS', 'HRL', 'TUL', 'RSW', 'ISP', 'AMA', 'BOS', 'MAF', 'EWR', 'LAS', 'BIS', 'JAN', 'ITO', 'XNA', 'DEN', 'ALB', 'CPR', 'LNK', 'PSP', 'BOI', 'SBA', 'IAD', 'SEA', 'MCI', 'BNA', 'CLT', 'TVC', 'BLI', 'ABQ', 'PBI', 'SDF', 'ACV', 'BDL', 'DAL', 'CLE', 'PDX', 'MIA', 'MFR', 'BWI', 'TPA', 'OKC', 'SMF', 'PHX', 'FCA', 'STL', 'PWM', 'MHT', 'DFW', 'GSP', 'HDN', 'LBB', 'CRP', 'FSD', 'SFO', 'MEM', 'ELP', 'BHM', 'FLL', 'ATL', 'RIC', 'OMA', 'VPS', 'LIT', 'ICT', 'FAT', 'ECP', 'ORD', 'BTV', 'BIL', 'PIA', 'RDU', 'MKE', 'XWA', 'SYR', 'PIT', 'MTJ', 'TUS', 'MDW', 'COS', 'IND', 'KOA', 'DTW', 'HOU', 'TYS', 'ONT', 'FWA', 'MDT', 'JAX', 'LAX', 'MSP', 'BTR', 'ROC', 'MCO', 'SGF', 'SAN', 'BZN', 'ANC', 'MSO', 'PHL', 'OGG', 'DSM', 'SAT', 'SLC', 'STT', 'RAP', 'BGM', 'PSE', 'GRB', 'GSO', 'FAR', 'COD', 'LWS', 'LEX', 'SCE', 'TRI', 'SPN', 'CAK', 'CHO', 'MOB', 'DIK', 'SLN', 'ERI', 'TLH', 'HPN', 'ATW', 'AVL', 'BFL', 'RIW', 'GFK', 'EYW', 'TTN', 'RST', 'MLB', 'PQI', 'FAI', 'DLH', 'HOB', 'BRO', 'DRO', 'BMI', 'LAN', 'BIH', 'PSC', 'ITH', 'MRY', 'DBQ', 'ACK', 'ILG', 'ROA', 'OTH', 'SPI', 'MBS', 'ABE', 'BFM', 'STX', 'FAY', 'GUC', 'HVN', 'EGE', 'SUN', 'FNT', 'DAY', 'PHF', 'CAE', 'AVP', 'ILM', 'GUM', 'BQN', 'MFE', 'LFT', 'HSV', 'AZO', 'OAJ', 'JNU', 'GPT', 'CHA', 'STS', 'MOT', 'BGR', 'AGS', 'DAB', 'JFK', 'GNV', 'SHR', 'ASE', 'SBP', 'PVU', 'PSG', 'SCC', 'RFD', 'WRG', 'IAG', 'EVV', 'KTN', 'CDV', 'ADK', 'SHV', 'TOL', 'HGR', 'GRI', 'OME', 'PBG', 'LRD',"PSM", "PIE", "SCK", "USA", "BRW", "SFB", "ELM", "BET", "SWF", "AZA", "AKN", "LCK", "STC", "SMX", "BLV", "CKB", "OWB", "ADQ", "HTS", "SIT", "OTZ", "YAK", "PGD", "DLG", "FNL", "GST", "HHH", "HYA", "FLG", "PAE", "CLL", "ORH", "SAF", "MVY", "PPG", "SBD", "VRB", "LBE", "ACY", "BKG", "EAU", "CWA", "SLE"]

dest_place = ["MSY", "GEG", "SNA", "BUR", "GTF", "GRR", "EUG", "MYR", "PVD", "OAK", "MSN", "DCA", "CID", "HLN", "RDM", "ORF", "SAV", "CMH", "PNS", "LIH", "IAH", "HNL", "CVG", "SJC", "LGA", "BUF", "AUS", "SJU", "ATW", "LGB", "GJT", "BFL", "RNO", "SRQ", "JAC", "CHS", "RSW", "HRL", "TUL", "ISP", "AMA", "BOS", "MAF", "EWR", "LAS", "JAN", "ITO", "FAI", "XNA", "DEN", "ALB", "PSP", "BOI", "SBA", "IAD", "SEA", "MCI", "BNA", "CLT", "TVC", "BLI", "PBI", "ABQ", "SDF", "BDL", "DAL", "CLE", "PDX", "MIA", "MFR", "TPA", "BWI", "OKC", "SMF", "PHX", "FCA", "STL", "PWM", "MHT", "DFW", "GSP", "HDN", "LBB", "CRP", "FSD", "SFO", "MEM", "ELP", "BHM", "FLL", "ATL", "RIC", "OMA", "VPS", "LIT", "FAT", "ICT", "ECP", "ORD", "BTV", "BIL", "RDU", "MFE", "MKE", "SYR", "PIT", "MTJ", "TUS", "MDW", "COS", "IND", "KOA", "HOU", "DTW", "ONT", "FWA", "MDT", "JAX", "LAX", "MSP", "BTR", "MCO", "ROC", "SAN", "BZN", "ANC", "MSO", "PHL", "OGG", "DSM", "SAT", "SLC", "STT", "RAP", "SBP", "BGM", "PSE", "GRB", "IDA", "GSO", "FAR", "COD", "LWS", "LEX", "SCE", "CRW", "TRI", "SPN", "CAK", "CHO", "MOB", "DIK", "ERI", "TLH", "RDD", "HPN", "MLI", "AVL", "RIW", "GFK", "SBN", "EYW", "TTN", "RST", "MLB", "PQI", "BIS", "DLH", "CPR", "LNK", "BRO", "DRO", "BMI", "LAN", "BIH", "PSC", "ACV", "ITH", "MRY", "DBQ", "ACK", "ILG", "PRC", "ROA", "OTH", "SPI", "MBS", "ABE", "BFM", "FAY", "STX", "GUC", "HVN", "EGE", "SWF", "SUN", "SAF", "FNT", "DAY", "PHF", "CAE", "AVP", "ILM", "PIA", "GUM", "BQN", "XWA", "HSV", "LFT", "AZO", "OAJ", "JNU", "GPT", "TYS", "CHA", "STS", "MOT", "BGR", "SGF", "DAB", "SUX", "JFK", "GNV", "SHR", "ASE", "PVU", "PSG", "SCC", "RFD", "WRG", "IAG", "EVV", "KTN", "CDV", "ADK", "SHV", "TOL", "HGR", "GRI", "OME", "PBG", "LRD", "PSM", "PAE", "PIE", "SCK", "USA", "BRW", "SFB", "ELM", "BET", "AZA", "LCK", "STC", "SMX", "BLV", "CKB", "OWB", "ADQ", "HTS", "SIT", "AGS", "OTZ", "YAK", "PGD", "DLG", "GST", "PUW", "HHH", "HYA", "HOB", "VEL", "ORH", "EAT", "AKN", "MVY", "COU", "YUM", "YKM", "PPG", "SBD", "SBY", "FLG", "VRB", "LBE", "ACY", "MGM", "VCT", "BKG", "EAU", "CWA", "SLE"]


  # 'ITIN_ID', 'QUARTER', 'ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'TK_CARRIER_count', 'MARKET_DISTANCE', 'MARKET_FARE'
def main(): 
    st.title("Airline Ticket Price Predictor")
    html_temp = """
    <div style="background:#025246 ;padding:10px">
    <h2 style="color:white;text-align:center;">Airline Price Prediction App </h2>
    </div>
    """
    st.markdown(html_temp, unsafe_allow_html = True)
    
    quarter = st.selectbox("QUARTER",["1", "2", "3", "4"]) 
    origin = st.selectbox("Origin Airport", origin_place) 
    dest = st.selectbox("Destination Airport", dest_place) 
    ticket_carrier = st.selectbox("Ticket Carrier",['UA','WN','XP','DL','F9','AA','G4','AS','B6','3M','NK','MX','HA','SY'])
    passengers = st.text_input("No. Passengers","1")
    tk_carrier_count = st.selectbox("Number of Carriers used",["1","2","3","4","5","6"])
    market_distance = st.text_input("Distance(Market Distance)","1")
    

    if st.button("Predict"): 
        # Create a PySpark DataFrame with the user inputs
        input_data = [(quarter, origin, dest, ticket_carrier, passengers, tk_carrier_count, market_distance)]
        input_columns = ['QUARTER', 'ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'TK_CARRIER_count', 'MARKET_DISTANCE'] 
        input_df = spark.createDataFrame(input_data, input_columns)
        
        transformed_input_df = dataPipeline.fit(input_df).transform(input_df)
        
        # Make predictions using the loaded model
        predictions = rfModel.transform(transoformed_input_df)

        # Extract the predicted 'MARKET_FARE' value
        market_fare = predictions.select("prediction").collect()[0][0]

        st.success(f'Employee Income is {market_fare}')
      
if __name__=='__main__': 
    main()