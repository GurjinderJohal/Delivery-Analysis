import pandas as pd
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
import time
import folium

file_path = 'Data Deliveries.xlsx'
df_2024 = pd.read_excel(file_path, sheet_name='2024')
df_2025 = pd.read_excel(file_path, sheet_name='2025')

#get total deliveries by each week
def getWeeklyDeliveries():

    df = pd.concat([df_2024,df_2025], ignore_index=True)

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    #start week from sunday instead of monday 
    df['Week'] = df['Date'].dt.to_period('W-SAT')
    weekly_counts = df.groupby('Week').size().reset_index(name='Total Deliveries')
    print(weekly_counts)

#generate a heat map for deliveries for specified week ex. 2025-04-28
def generateHeatMap(start_of_week):

    df = df_2025
    df['Date'] = pd.to_datetime(df['Date'])

    start_date = pd.to_datetime(start_of_week)
    end_date = start_date + timedelta(days=6)

    df_week = df[(df['Date'] >= start_date) & (df['Date'] <= end_date) ]

    addresses = list(df_week['Address'])

    #get latitude and longitude of addresses
    locations = get_locations(addresses)

    df_locations = pd.DataFrame(locations)
    df_locations = df_locations.rename(columns={0:'Address',1:'latitude',2:'longitude'})
    
    #merge the latitude and longitude
    df_week = df_week.merge(df_locations,on='Address',how='left')
    df_week = df_week.dropna(subset=['latitude', 'longitude'])

    map_center = [df_week['latitude'].mean(),df_week['longitude'].mean()]

    m = folium.Map(location=map_center, zoom_start=12)

    #generate heatmap
    heat_data = df_week[['latitude','longitude']]
    HeatMap(heat_data).add_to(m)

    m.save("delivery_heatmap_week.html")
    print("Heatmap saved")

#get latitude and longitude for addresses
def get_locations(addresses):
    geolocator = Nominatim(user_agent="address_mapper")
    locations = []
    for address in addresses:
        try:
            location = geolocator.geocode(address)
            if location:
                locations.append((address, location.latitude, location.longitude))
        except Exception as e:
            print(f"Error geocdoing {address}: {e}")
        time.sleep(2)
    return locations
    
#get inactive customers that have not ordered in the last 6 months
def getInactiveCustomers():
    
    # Combine both DataFrames
    df = pd.concat([df_2024, df_2025], ignore_index=True)
    # Ensure 'date' column is datetime type
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    # Remove any rows where date could not be parsed
    df = df.dropna(subset=['Date'])

    # Calculate the date 6 months ago from today
    six_months_ago = datetime.now() - pd.DateOffset(months=6)

    # Get the most recent order date per address
    last_order_per_address = df.groupby('Address')['Date'].max().reset_index()

    # Find addresses where the last order was more than 6 months ago
    inactive_customers = last_order_per_address[last_order_per_address['Date'] < six_months_ago]

    # Merge with original data to get full order history for inactive customers
    merged = pd.merge(inactive_customers[['Address']],df, on='Address',how='left')
    
    # Calculate order counts per address
    order_counts = merged.groupby('Address').size().reset_index(name='order_count')

    # Filter to only those customers who have ordered more than once
    multi_order_customers = order_counts[order_counts['order_count'] > 1]
    
    # Calculate average ticket price for these multi-order inactive customers
    avg_ticket_price = merged[merged['Address'].isin(multi_order_customers['Address'])] \
                    .groupby('Address')['Amount'].mean().reset_index().rename(columns={'Amount': 'avg_ticket_price'})
    

    inactive_customers = pd.merge(avg_ticket_price, last_order_per_address, on='Address')

    # Show the addresses that haven't ordered in the last 6 months
    print("Customers who haven't ordered in the last 6 months and have had one:")
    print(inactive_customers)
    #save the inactive customers to excel file
    inactive_customers.to_excel('inactive_customers.xlsx', index=False)

#map the inactive customers to see a visual representation 
def mapInactiveCustomers():

    getInactiveCustomers()
    inactive_customers = pd.read_excel('inactive_customers.xlsx')
    addresses = list(inactive_customers['Address'])

    #grab latitude and longitude of addresses
    locations = get_locations(addresses)

    if locations:
        avg_lat = sum(lat for _, lat, _ in locations) / len(locations)
        avg_lon = sum(lon for _,_, lon in locations) / len(locations)
        m = folium.Map(location=[avg_lat, avg_lon], zoom_start=10)
    
        for addr, lat, lon in locations:
            folium.Marker(location=[lat,lon], popup=addr).add_to(m)
        m.save("Inactive Customers.html")
        print("Map saved as Inactive Customers.html")
    else:
        print("No locations to map.")


