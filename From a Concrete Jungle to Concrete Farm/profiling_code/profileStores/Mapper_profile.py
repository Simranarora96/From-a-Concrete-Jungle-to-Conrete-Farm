#!/usr/bin/env python
import re
import sys

def main():
        maximum = 0
        minimum = float("inf")
        for line in sys.stdin:
                val = line.strip().split(",")
                (County,City, State, Zip_Code,Square_Footage)=(val[0][2:-1],val[1][2:-1],val[2][2:-1],val[3][0:],val[4][0:-1])

                Zip_Code,Square_Footage = int(Zip_Code),int(Square_Footage)
                County,City,State = str(County),str(City),str(State)

                if Square_Footage > maximum:
                        maximum = Square_Footage

                if Square_Footage < minimum and Square_Footage>100:
                        minimum = Square_Footage

        print("maximum =", maximum)
        print("minimum =", minimum)

if __name__=="__main__":
        main()
