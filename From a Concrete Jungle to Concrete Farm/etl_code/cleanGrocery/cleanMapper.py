#!/usr/bin/env python
import sys
import geopandas as gpd
from shapely.geometry import Point
import googlemaps

secret_key = '<YOUR_SECRET_KEY>''
gmaps = googlemaps.Client(key=secret_key)

for line in sys.stdin:
	line = line.strip()
	fields = line.split(',')
	latitude = fields[9]
	longitude = fields[10]
	address = fields[3].strip() + " " + fields[4].strip() + " " + fields[5].strip() + " " + fields[6].strip() + " " + fields[7].strip()

	location_existed = True
	p = None
	parentNTA = ','
	if latitude == '' or longitude == '': # no location, use google maps to discover\
		location_existed = False
		result = gmaps.geocode(address)
		if len(result) < 1:
			# no location could be found
			fields[9] = "XXX"
			fields[10] = "XXX"
			parentNTA += "XXXX"
			continue # comment out to include bad results
		else:
			lng = result[0]['geometry']['location']['lng']
			lat = result[0]['geometry']['location']['lat']
			fields[9] = str(lat)
			fields[10] = str(lng)
			p = Point(lng, lat)
	else:
		p = Point(float(longitude), float(latitude))

	#fp = "hdfs://dumbo/user/jma587/project/NeighborhoodTabulationAreas/geo_export_7364d06a-cf46-4bbb-999c-b08b0c81c035.shp"
	fp = "geo_export_7364d06a-cf46-4bbb-999c-b08b0c81c035.shp"
	polys = gpd.read_file(fp)

	if p:
		for i in range(len(polys)):
			if (p.within(polys.loc[i,'geometry'])):
				parentNTA += (polys.loc[i, 'ntacode']).strip()
				break

	if len(parentNTA) == 1 and location_existed: # if you don't find a parent, use google maps to look up your address and try again
		result = gmaps.geocode(address)
		if len(result) < 1:
			# no location could be found
			parentNTA += "XXXX"
		else:
			lng = result[0]['geometry']['location']['lng']
			lat = result[0]['geometry']['location']['lat']
			p = Point(lng, lat)

			for i in range(len(polys)):
				if (p.within(polys.loc[i,'geometry'])):
					parentNTA += (polys.loc[i, 'ntacode']).strip()
					break
			if len(parentNTA) <= 1:
				parentNTA += "UNKNOWN"
				continue # comment out to include bad results
	elif len(parentNTA) == 1 and (not location_existed):
		parentNTA += "UNKOWN"
		continue # comment out to include bad results

	res = ','.join(fields)
	res += parentNTA
	print("0\t" + res)
