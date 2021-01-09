#!/usr/bin/env python
import re
import sys

def main():

        for line in sys.stdin:
                val = line.strip().split(",")
                (County,City, State, Zip_Code,Square_Footage,Latitude) = (val[0],val[10],val[11],val[12],val[13], val[17:])
                if (County=="Bronx" or County=="Kings" or County=="New York" or County=="Queens" or County=="Richmond") and State=="NY" and Square_Footage != '0'  :
                        Latitude, Longitude= Latitude[0][2:], Latitude[1][:-2]
                        Square_Footage,Zip_Code,Latitude, Longitude = int(Square_Footage), int(Zip_Code), float(Latitude), float(Longitude)
                        print (County, City, State, Zip_Code, Square_Footage,Latitude, Longitude)



if __name__=="__main__":
        main()
