import pandas as pd
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
import time
import folium
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import StandardScaler

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


# Convert time (e.g., 14:30:00) into seconds since midnight
def time_to_seconds(t):
    if pd.isnull(t):
        return None
    return t.hour * 3600 + t.minute * 60 + t.second

#use supervised machine learning to find likelihood of customer reordering
#use RandomForestModel
def customerRetention():
    df = pd.concat([df_2024,df_2025], ignore_index=True)
    # Ensure 'date' column is datetime
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])

    # Use April 1, 2025 as reference point
    reference_date = pd.Timestamp('2025-04-01')
    six_months_ago = reference_date - pd.DateOffset(months=6)

    df['time_in_seconds'] = df['Time'].apply(time_to_seconds)

    # Group by customer (address)
    group = df.groupby('Address').agg({
        'Date': ['min', 'max', 'count'],
        'Amount': 'mean',
        'Channel': lambda x: x.value_counts().to_dict(),
        'time_in_seconds': 'mean'
    })

    # Flatten column names
    group.columns = ['first_order', 'last_order', 'total_orders', 'avg_order_amount', 'order_channels', 'avg_order_time']
    group.reset_index(inplace=True)

    # Add days since last order
    group['days_since_last_order'] = (reference_date - group['last_order']).dt.days

    # Create churn label: 1 = churned (no order in last 6 months)       
    group['churn'] = (group['last_order'] < six_months_ago).astype(int)

    # Expand order channels into separate columns
    channels_df = pd.json_normalize(group['order_channels'])
    group = pd.concat([group, channels_df.fillna(0)], axis=1)

    # Final features
    features = ['total_orders', 'avg_order_amount', 'days_since_last_order', 'avg_order_time']
    features += [col for col in ['POS', 'call Center', 'web', 'mobile'] if col in group.columns]

    # Drop rows with missing data in selected features
    X = group[features].fillna(0)
    y = group['churn']

    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

    # Train Random Forest model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Evaluate model
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    print(classification_report(y_test, y_pred))
    print("ROC AUC Score:", roc_auc_score(y_test, y_prob))

    # Predict churn probability for all customers
    group['churn_probability'] = model.predict_proba(X_scaled)[:, 1]
    group['churn_prediction'] = (group['churn_probability'] >= 0.5).astype(int)

    # Save to Excel
    group[['Address', 'total_orders', 'avg_order_amount', 'days_since_last_order',
        'churn_probability', 'churn_prediction']].to_excel('churn_model_output.xlsx', index=False)

    #1 if they are likely to churn, 0 if not
    print("Churn predictions saved to 'churn_model_output.xlsx'")


