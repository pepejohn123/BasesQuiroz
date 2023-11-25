#!/usr/bin/env python

"""
Generador de datos para proyecto de Bases de Datos No Relacionales
ITESO 
"""
import argparse
import csv
import datetime

from random import choice, randint, randrange

airports_names = "RAI,CPT,JNB,ALG,AAE,CZL,ORN,BUG,CAB,LAD,COO,FRW,GBE,MUB,PKW,BOY,OUA,SID,BBY,BGU,BGF,BBT,IRO,BIV,CRF,ODA,AEH,MQQ,NDJ,AJN,HAH,YVA,BZV,PNR,FIH,LIQ,ASK,JIB,AUE,ABS,AAC,AAC,ALY,ATZ,ASW,HBH,CAI,EMY,HRG,UVL,LXR,MUH,UVL,PSD,SKV,SSH,SEW,SSG,ADD,LBQ,LBV,MFF,MJL,MVB,POG,UVE,BJL,ACC,ABJ,BYK,DJO,HGO,MJC,SPY,ZSS,MYD,MBA,NBO,MSU,MLW,ROB,BEN,SEB,TIP,TNR,MJN,BLZ,LLW,BKO,NDB,NKC,OUZ,DZA,AGA,AHU,CAS,CMN,FEZ,RAK,OZZ,OUD,RBA,TNG,BEW,MPM,MPA,KMP,LUD,OKU,OND,OMD,NDU,SWP,TSB,ERS,WDH,AJY,RLT,MFQ,NIM,ZND,ABV,KAN,LOS,PHC,RUN,KGL,TMS,DKR,SEZ,FNA,MGQ,AGZ,ALJ,ADY,BFN,DUR,ELS,ELL,GRJ,KIM,KLZ,HLA,LUJ,MGH,MEZ,MBM,MZF,NLP,NCS,OUH,PHW,PZB,PTG,NTY,PBZ,PLZ,PRY,RCB,SIS,SZK,SBU,TCU,ULD,UTT,UTN,VYD,WVB,WEL,KSL,KRT,PBM,MTS,ARK,DAR,JRO,DJE,MIR,SFA,TUN,EBB,ULU,FKI,FBM,CIP,KIW,LUN,MFU,NLA,BFO,BUQ,GWE,HRE,HWN,MVZ,SAY,VFA,SPK,OKD,HKG,UKB,UKY,NGO,TYO,HND,NRT,MLE,KTM,ICN,SEL,CMB,BZL,CGP,DAC,ZYL,PBH,PEK,NAY,SHA,PVG,TSN,XMN,DGM,CAN,SZX,NNG,HRB,ZJK,WUH,AMD,ATQ,QNB,IXB,BLR,BDQ,IXG,BHO,BBI,BOM,CCU,CCJ,IXC,MAA,COK,CJB,DEL,GOI,GAU,HYD,JAI,JLR,IXW,QJU,CCU,LKO,MAA,NAG,PAT,PNQ,RAJ,IXR,SXR,STV,TRV,TRZ,VNS,AXT,ASJ,AOJ,KMQ,QCB,CTS,FUK,FKS,HAC,HKD,HIJ,LSG,KOJ,KCZ,KMJ,KUH,MMJ,MYJ,MMY,HNA,KMI,NGS,KIJ,OIT,OKJ,OKA,OSA,ITM,KIX,SDS,CTS,SDJ,TAK,TKS,TOY,GAJ,YOK,CGQ,ADX,ALA,TSE,ICN,DLC,SHE,MFM,MRU,RRG,ULN,XIY,TNA,TAO,TYN,CTU,CKG,QPG,SIN,PUS,DYU,KHH,MZG,TPE,TAY,SKD,TAS,TMZ,URC,HGH,PTP,BON,CUR,SXM,NEV,SKB,UVF,SLU,SFG,SVD,HAV,HOG,SCU,VRA,LRM,POP,PUJ,SDQ,PAP,KIN,MBJ,FDF,BQN,MAZ,PSE,SJU,TAB,POS,STX,STT,EIS,VIJ,EIS,FPO,NAS,GCM,AXA,ANU,AUA,CCZ,GHB,GBI,MHH,ELH,RSD,ZSA,TCB,BGI,BDA,PTY,SJO,SAL,RTB,SAP,SDH,TGU,BZE,GUA,MGA,SJJ,SOF,NAN,SUV,SUV,BUD,SKP,BUH,OTP,EVN,BAK,MSQ,OMO,BOJ,GOZ,ROU,SLS,TGV,VAR,VID,DBV,LSZ,OSI,PUY,RJK,SPU,ZAD,ZAG,QUF,TLL,TBS,RIX,VNO,OHD,CND,AER,KHV,HTA,IKT,KZN,MRV,MOW,DME,SVO,VKO,MMK,OVB,LED,UUD,VLU,ARH,YKS,BTS,LJU,MBX,KBP,IEV,LWO,NLV,ODS,SIP,BEG,INI,QND,TGD,PRN,TIV,TIA,INN,SZG,VIE,CPH,HEL,BER,SXF,TXL,DRS,HAM,ATH,HEW,CFU,KGS,JMK,MJT,RHO,SKG,IBZ,ORK,DUB,GWY,KIR,NOC,SNN,CAG,MLA,BGO,OSL,TRF,KRK,WAW,LIS,PDL,PMI,SVQ,VLC,GOT,STO,ARN"
airports = airports_names.split(',')

# Print the result
print("The length of the list is:", len(airports))
airlines_names = "Aegean Airlines,Aer Lingus,Aeroflot,Aerolineas Argentinas,Aeromexico,Air Arabia,Air Astana,Air Austral,Air Baltic,Air Belgium,Air Canada,Air Caraibes,Air China,Air Corsica,Air Dolomiti,Air Europa,Air France,Air India,Air India Express,Air Macau,Air Malta,Air Mauritius,Air Namibia,Air New Zealand,Air North,Air Seoul,Air Serbia,Air Tahiti Nui,Air Transat,Air Vanuatu,AirAsia,AirAsia X,Aircalin,Alaska Airlines,Alitalia,Allegiant,American Airlines,ANA,Asiana,Austrian,Avianca,Azerbaijan Hava Yollary,Azores Airlines,Azul,Bamboo Airways,Bangkok Airways,British Airways,Brussels Airlines,Caribbean Airlines,Cathay Dragon,Cathay Pacific,Cayman Airways,CEBU Pacific Air,China Airlines,China Eastern,China Southern,Condor,Copa Airlines,Croatia Airlines,Czech Airlines,Delta,easyJet,Edelweiss Air,Egyptair,EL AL,Emirates,Ethiopian Airlines,Etihad,Eurowings,EVA Air,Fiji Airways,Finnair,flydubai,FlyOne,French bee,Frontier,Garuda Indonesia,Gol,Gulf Air,Hainan Airlines,Hawaiian Airlines,Helvetic Airways,HK Express,Hong Kong Airlines,Iberia,Icelandair,IndiGo Airlines,InterJet,Japan Airlines,Jeju Air,Jet2,JetBlue,Jetstar,Jin Air,Kenya Airways,KLM,Korean Air,Kulula,La Compagnie,LATAM,Lion Airlines,LOT Polish Airlines,Lufthansa,Luxair,Malaysia Airlines,Mango,Middle East Airlines,Nok Air,Nordwind Airlines,Norwegian Air International,Norwegian Air Shuttle,Norwegian Air Sweden,Norwegian Air UK,Oman Air,Pakistan International Airlines,Peach,Pegasus Airlines,Philippine Airlines,Porter,Qantas,Qatar Airways,Regional Express,Rossiya - Russian Airlines,Royal Air Maroc,Royal Brunei,Royal Jordanian,RwandAir,Ryanair,S7 Airlines,SAS,Saudia,Scoot Airlines,Shanghai Airlines,Silkair,Silver,Singapore Airlines,Skylanes,South African Airways,Southwest,SpiceJet,Spirit,Spring Airlines,Spring Japan,SriLankan Airlines,Sun Country,Sunclass Airlines,Sunwing,SWISS,Swoop,TAAG,TACA,TAP Portugal,THAI,tigerair Australia,Transavia Airlines,TUI UK,TUIfly,Tunis Air,Turkish Airlines,Ukraine International,United,Ural Airlines,UTair Aviation,Uzbekistan Airways,Vietnam Airlines,Virgin Atlantic,Virgin Australia,Vistara,Viva Aerobus,Volaris,Volotea,Vueling Airlines,WestJet,Wizzair,Xiamen Airlines"
airlines = airlines_names.split(',')
print("The length of the list is:", len(airlines))
genders = ["male", "female", "unspecified", "undisclosed"]
reasons = ["On vacation/Pleasure", "Business/Work", "Back Home"]
stays = ["Hotel", "Short-term homestay", "Home", "Friend/Family"]
not_home_stays  = ["Hotel", "Short-term homestay", "Friend/Family"]

transits = ["Airport cab", "Car rental", "Mobility as a service", "Public Transportation", "Pickup", "Own car"]
tourist_transits = ["Airport cab", "Car rental", "Mobility as a service", "Public Transportation", "Pickup"]

home_transits = ["Airport cab", "Mobility as a service", "Public Transportation", "Pickup", "Own car"] ##no rental car


connections = [True, False]


def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = randrange(days_between_dates)
    rand_date = start_date + datetime.timedelta(days=random_number_of_days)
    return rand_date


def generate_dataset(output_file, rows):
    with open(output_file, "w") as fd:
        fieldnames = ["airline", "from" , "to", "day", "month", "year", "age", "gender", "reason", "stay", "transit", "connection", "wait"]
        fp_dict = csv.DictWriter(fd, fieldnames=fieldnames)
        fp_dict.writeheader()
        for i in range(rows):
            from_airport = choice(airports)
            to_airport = choice(airports)
            while from_airport == to_airport:
                to_airport = choice(airports)
            date = random_date(datetime.datetime(2013, 1, 1), datetime.datetime(2023, 4, 25))
            reason = choice(reasons)
            stay = choice(stays)
            connection = choice(connections)
            wait = randint(30, 720)
            transit = choice(transits)
            if not connection:
               wait = 0
            else:
                transit = ""
            if reason == "Back Home":
                stay = "Home"
                connection = False
                wait = 0
                transit = choice(home_transits)
            if reason !="Back Home":
                stay = choice(not_home_stays)
                transit = choice(tourist_transits)


                
            line = {
                "airline": choice(airlines),
                "from":  from_airport,
                "to":  to_airport,
                "day": date.day,
                "month": date.month,
                "year": date.year,
                "age": randint(1,90),
                "gender": choice(genders),
                "reason": reason,
                "stay": stay,
                "transit": transit,
                "connection": connection,
                "wait": wait,
            }
            fp_dict.writerow(line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-o", "--output",
            help="Specify the output filename of your csv, defaults to: flight_passengers.csv", default="flight_passengers.csv")
    parser.add_argument("-r", "--rows",
            help="Amount of random generated entries for the dataset, defaults to: 100", type=int, default=100)

    args = parser.parse_args()
    
    print(f"Generating {args.rows} for flight passenger dataset")
    generate_dataset(args.output, args.rows)
    print(f"Completed generating dataset in {args.output}")


